#
#//  web_transport.py
#//  silverbrain
#//
#//  Created by Evan Mason on 4/8/26.
#

import io
import json
import threading
from queue import Empty, Queue
from threading import Lock
from typing import Callable, Self

import polars as pl
import zmq

from .types import WebProtocol

# -- Serialization

def pack_envelope(
    web_id: str,
    op_id: str
    ) -> bytes:
    return json.dumps({
        'web_id': web_id,
        'op_id': op_id,
    }).encode()
#/pack_envelope

def unpack_envelope(
    data: bytes,
    ) -> tuple[
        str, # web_id
        str, # op_id
    ]:
    d = json.loads( data )
    return d[ 'web_id' ], d[ 'op_id' ]
#/unpack_envelope

def pack_dataframes(
    dfs: tuple[
        pl.DataFrame,
        ...
    ]
    ) -> list[
        bytes
    ]:
    frames = []
    for df in dfs:
        buf = io.BytesIO()
        df.write_ipc( buf )
        frames.append( buf.getvalue() )
    #/for df in dfs
    
    return frames
#/pack_dataframes

def unpack_dataframes(
    frames: list[ bytes ]
    ) -> tuple[
        pl.DataFrame,
        ...
    ]:
    return tuple(
        pl.read_ipc(
            io.BytesIO( f )
        ) for f in frames
    )
#/unpack_dataframes

# -- SendOp factory

def make_send_op(
    dest_web_id: str,
    dest_op_id: str,
    outbox: Queue,
    ) -> Callable[
        [
            tuple[ pl.DataFrame,... ], # dfs
            int, # verbose
            str, # verbose_prefix
        ],
        tuple[()]
    ]:
    """
        Returns a tableOp that puts its dfs into outbox with routing info, then
        returns an empty tuple. Use as the `op` in a TableProcessRef with target=[].

        Example:
            TableProcessRef(
                source=['result'],
                target=[],
                op=make_send_op('web_b', 'compute', web.outbox),
                opId='send_result',
            )
    """
    def send_op(
        dfs: tuple[ pl.DataFrame, ... ],
        verbose: int = 0,
        verbose_prefix: str = '',
        ) -> tuple[()]:
        outbox.put( ( dest_web_id, dest_op_id, dfs ) )
        return ()
    #/send_op
    return send_op
#/make_send_op

# -- WebSender

class WebSender:
    """
        Sends DataFrame tuples to remote Webs via ZMQ PUSH sockets.
        Connections are opened lazily by address on first send and pooled.
        For inproc:// transport, pass the same zmq.Context as the WebReceiver.
    """
    def __init__(
        self: Self,
        context: zmq.Context | None = None
        ) -> None:
        self._ctx = context or zmq.Context()
        self._own_ctx = context is None
        self._sockets: dict[ str, zmq.Socket ] = {}
        self._lock = Lock()

        return
    #/def __init__

    def send(
        self: Self,
        address: str,
        web_id: str,
        op_id: str,
        dfs: tuple[ pl.DataFrame, ... ],
        ) -> None:
        frames = [
            pack_envelope( web_id, op_id )
        ] + pack_dataframes( dfs )

        with self._lock:
            if address not in self._sockets:
                sock = self._ctx.socket( zmq.PUSH )
                sock.connect( address )
                self._sockets[ address ] = sock
            #/if address not in self._sockets

            self._sockets[ address ].send_multipart( frames )
        #/with self._lock

        return
    #/def send

    def close(
        self: Self
        ) -> None:
        
        for sock in self._sockets.values():
            sock.close()
        #
        
        if self._own_ctx:
            self._ctx.term()
        #
        return
    #/def close
#/class WebSender

# -- WebReceiver

class WebReceiver:
    """
        Listens on a ZMQ PULL socket and places (op_id, dfs) tuples into web.inbox.
        Only messages whose op_id is in web.inputIds are accepted; others are dropped.
        For inproc:// transport, pass the same zmq.Context as the WebSender.
    """
    def __init__(
        self: Self,
        web: WebProtocol,
        address: str,
        context: zmq.Context | None = None,
        ) -> None:
        self._web: WebProtocol = web
        self._address = address
        self._ctx = context or zmq.Context()
        self._own_ctx = context is None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
    #/def __init__

    def start(
        self: Self,
        ) -> None:
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True
        )
        self._thread.start()
        
        return
    #/def start

    def stop(
        self: Self,
        ) -> None:
        
        self._stop.set()
        
        return
    #/def stop

    def _run(
        self: Self,
        ) -> None:
        
        sock = self._ctx.socket( zmq.PULL )
        sock.setsockopt( zmq.RCVTIMEO, 200 )
        sock.bind( self._address )
        
        try:
            while not self._stop.is_set():
                try:
                    frames = sock.recv_multipart()
                except zmq.Again:
                    continue
                #/try/except zmg.Again
                
                web_id, op_id = unpack_envelope(
                    frames[ 0 ]
                )
                
                if op_id not in self._web.inputIds:
                    continue
                #
                
                dfs = unpack_dataframes(
                    frames[ 1: ]
                )

                self._web.inbox.put(
                    ( op_id, dfs )
                )
                self._web.notify()
            #/while not self._stop.is_set()
        finally:
            sock.close()
            if self._own_ctx:
                self._ctx.term()
            #/if self._own_ctx
        #/try/finally
        return
    #/def _run
#/class WebReceiver

# -- OutboxSender

class OutboxSender:
    """
        Background thread that reads (dest_web_id, dest_op_id, dfs) tuples from
        an outbox queue, resolves the destination address from web.tables['routes']
        at dispatch time, and sends via a WebSender.

        Routes are re-read on every send so live route updates (e.g. from a router
        web push) take effect automatically.

        By default monitors web.outbox.  Pass an explicit ``outbox`` to monitor a
        different queue while still reading routes from web.tables — useful when
        tableProcesses captured an original outbox queue before the web was combined.
    """
    def __init__(
        self: Self,
        web: WebProtocol,
        sender: WebSender,
        outbox: 'Queue | None' = None,
        ) -> None:

        self._web: WebProtocol = web
        self._sender = sender
        self._outbox = outbox if outbox is not None else web.outbox
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        return
    #/def __init__

    def start(
        self: Self
        ) -> None:
        
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
        )
        self._thread.start()
        
        return
    #/def start

    def stop(
        self: Self,
        ) -> None:
        
        self._stop.set()
        
        return
    #/def stop

    def _dispatch_item(
        self: Self,
        item: tuple,
        ) -> None:

        dest_web_id, dest_op_id, dfs = item

        routes = self._web.tables.get( 'routes' )

        if routes is None:
            return
        #

        match = routes.filter(
            ( pl.col( 'web_id' ) == dest_web_id ) &
            ( pl.col( 'op_id' )  == dest_op_id  )
        )

        if match.is_empty():
            return
        #

        address = match[ 'address' ][ 0 ]
        self._sender.send(
            address,
            dest_web_id,
            dest_op_id,
            dfs,
        )
    #/def _dispatch_item

    def _run(
        self: Self
        ) -> None:

        while not self._stop.is_set():
            try:
                item = self._outbox.get( timeout=0.2 )
            except Empty:
                continue
            #/try/except Empty
            self._dispatch_item( item )
        #/while not self._stop.is_set()

        # Drain any items that arrived before the stop signal was detected
        while True:
            try:
                item = self._outbox.get_nowait()
            except Empty:
                break
            self._dispatch_item( item )
        #/drain

        return
    #/def _run
#/class OutboxSender
