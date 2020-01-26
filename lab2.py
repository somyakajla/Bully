"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Kevin Lundeen
:Version: f19-02
"""
import pickle
import selectors
import socket
import sys
from datetime import datetime
from enum import Enum

BUF_SZ = 4096  # default socket.recv buffer size
BACKLOG = 100  # socket.listen(BACKLOG) for our server
ASSUME_FAILURE_TIMEOUT = 2.0  # seconds without response assumes failure of peer
CHECK_INTERVAL = 0.2  # seconds between checking for failure timeout
PEER_DIGITS = 100  # modulo of peer port number for log (100 is useful for diagnosis)


class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)


class Lab2(object):
    """
    Class to perform the specified behavior for Lab 2.
    """

    def __init__(self, gcd_address, next_birthday, su_id):
        """
        Constructs a Lab2 object to talk to the given Group Coordinator Daemon.

        :param gcd_address: host name and port of the GCD
        :param next_birthday: datetime of next birthday
        :param su_id: SeattleU id number
        """
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birthday - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))
        self.members = {}
        self.states = {}
        self.bully = None  # None means election is pending, otherwise this will be pid of leader
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

    def run(self):
        """
        Do the work of a group member as specified for Lab 2.
        """
        print('STARTING WORK for pid {} on {}'.format(self.pid, self.listener_address))
        self.join_group()
        self.selector.register(self.listener, selectors.EVENT_READ)
        self.start_election('at startup')
        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)
            self.check_timeouts()

    def accept_peer(self):
        """Accept new TCP/IP connections from a peer."""
        try:
            peer, _addr = self.listener.accept()  # should be ready since selector said so
            print('{}: accepted [{}]'.format(self.pr_sock(peer), self.pr_now()))
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, peer)
        except Exception as err:
            print('accept failed {}'.format(err))

    def send_message(self, peer):
        """
        Send the queued message to the given peer (based on its current state).

        :param peer: socket connected to peer process
        """
        state = self.get_state(peer)
        print('{}: sending {} [{}]'.format(self.pr_sock(peer), state.value, self.pr_now()))
        try:
            self.send(peer, state.value, self.members)  # should be ready, but may be a failed connect instead

        except ConnectionError as err:
            # a ConnectionError means it will never succeed
            print('closing: {}'.format(err))
            self.set_quiescent(peer)
            return

        except Exception as err:
            # other exceptions we assume are due to being non-blocking; we expect them to succeed in future
            print('failed {}: {}'.format(err.__class__.__name__, err))
            if self.is_expired(peer):
                # but if we've kept trying and trying to no avail, then give up
                print('timed out')
                self.set_quiescent(peer)
            return

        # check to see if we want to wait for response immediately
        if state == State.SEND_ELECTION:
            self.set_state(State.WAITING_FOR_OK, peer, switch_mode=True)
        else:
            self.set_quiescent(peer)

    def receive_message(self, peer):
        """
        Receive a message from a peer and do the corresponding state transition.

        :param peer: socket connected to peer process
        """
        try:
            message_name, their_idea = self.receive(peer)
            print('{}: received {} [{}]'.format(self.pr_sock(peer), message_name, self.pr_now()))

        except ConnectionError as err:
            # a ConnectionError means it will never succeed
            print('closing: {}'.format(err))
            self.set_quiescent(peer)
            return

        except Exception as err:
            # other exceptions we assume are due to being non-blocking; we expect them to succeed in future
            print('failed {}'.format(err))
            if self.is_expired(peer):
                # but if we've kept trying and trying to no avail, then give up
                print('timed out')
                self.set_quiescent(peer)
            return

        self.update_members(their_idea)  # add any new members

        # handle state transition
        if message_name == 'ELECTION':
            self.set_state(State.SEND_OK, peer)
            if not self.is_election_in_progress():
                self.start_election('Got a VOTE card from a lower-pid peer')

        elif message_name == 'COORDINATOR':
            # election is over (though I might not even have known there was one!)
            self.set_leader('somebody else')  # FIXME - should change the protocol to indicate WHO is the new leader
            self.set_quiescent(peer)
            self.set_quiescent()

        elif message_name == 'OK':
            # if I have yet to get any oks since starting election and I get an OK, I change to expecting COORDINATOR
            if self.get_state() == State.WAITING_FOR_OK:
                self.set_state(State.WAITING_FOR_VICTOR)  # subsequent OKs are ignored since already waiting
            self.set_quiescent(peer)

    def check_timeouts(self):
        """Check to see if we need to do a state transition because of peer failures."""
        if self.is_expired():  # we only need to check ourselves
            if self.get_state() == State.WAITING_FOR_OK:
                self.declare_victory('timed out waiting for any OK message')
            else:  # my_state == State.WAITING_FOR_VICTOR:
                self.start_election('timed out waiting for COORDINATION message')

    def get_connection(self, member):
        """
        Get a socket for given member.
        Note that the connection will be done in non-blocking mode, so may not be available immediately--use select
        to pick it up when it becomes writable.

        :param member: process id of peer, i.e., (daystobd, suid)
        :return: socket (or None if failed)
        """
        listener = self.members[member]  # look up address to connect to
        peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer.setblocking(False)
        try:
            peer.connect(listener)

        except BlockingIOError:
            pass  # connection still in progress -- will pick it up in select

        except Exception as err:
            print('FAILURE: get connection failed: {}'.format(err))
            return None

        return peer

    def is_election_in_progress(self):
        """Check if we are currently in an election (signified by bully of None)."""
        return self.bully is None

    def is_expired(self, peer=None, threshold=ASSUME_FAILURE_TIMEOUT):
        """
        Check if given peer's state was set more than threshold seconds ago.

        :param peer: socket connected to peer process (None means self)
        :param threshold: seconds to wait since last state transition
        :return: True if elapsed time is past threshold, False otherwise (or if state is quiescent)
        """
        my_state, when = self.get_state(peer, detail=True)
        if my_state == State.QUIESCENT:
            return False
        waited = (datetime.now() - when).total_seconds()
        return waited > threshold

    def set_leader(self, new_leader):
        """
        Set the leader (handy place to put a log print).

        :param new_leader: whatever you want self.bully to be set to
        """
        self.bully = new_leader
        print('Leader is now {}'.format(self.pr_leader()))

    def get_state(self, peer=None, detail=False):
        """
        Look up current state in state table.

        :param peer: socket connected to peer process (None means self)
        :param detail: if True, then the state and timestamp are both returned
        :return: either the state or (state, timestamp) depending on detail (not found gives (QUIESCENT, None))
        """
        if peer is None:
            peer = self
        status = self.states[peer] if peer in self.states else (State.QUIESCENT, None)
        return status if detail else status[0]

    def set_state(self, state, peer=None, switch_mode=False):
        """
        Set/change the state for the given process.

        :param State state: new state
        :param peer: socket connected to peer process (None means self)
        :param switch_mode: True if we want to move from read to write or vice versa FIXME: autodetect this?
        """
        print('{}: {}'.format(self.pr_sock(peer), state.name))
        if peer is None:
            peer = self

        # figure out if we need to register for read or write in selector
        if state.is_incoming():
            mask = selectors.EVENT_READ
        else:
            mask = selectors.EVENT_WRITE

        # setting to quiescent means delete it from self.states
        if state == State.QUIESCENT:
            if peer in self.states:
                if peer != self:
                    self.selector.unregister(peer)
                del self.states[peer]
            if len(self.states) == 0:
                print('{} (leader: {})\n'.format(self.pr_now(), self.pr_leader()))
            return

        # note the new state and adjust selector as necessary
        if peer != self and peer not in self.states:
            peer.setblocking(False)
            self.selector.register(peer, mask)
        elif switch_mode:
            self.selector.modify(peer, mask)
        self.states[peer] = (state, datetime.now())

        # try sending right away (if currently writable, the selector will never trigger it)
        if mask == selectors.EVENT_WRITE:
            self.send_message(peer)

    def set_quiescent(self, peer=None):
        """Shortcut for set_state(QUIESCENT)."""
        self.set_state(State.QUIESCENT, peer)

    def start_election(self, reason):
        """
        Sends an ELECTION to all the greater group members.
        If there are no greater members, then we declare victory.
        Otherwise we wait for OKs.

        :param reason: text to put out to log as explanation
        """
        print('Starting an election {}'.format(reason))
        self.set_leader(None)  # indicates that election is in progress
        self.set_state(State.WAITING_FOR_OK)
        i_am_biggest_bully = True
        for member in self.members:
            if member > self.pid:
                peer = self.get_connection(member)
                if peer is None:
                    continue
                self.set_state(State.SEND_ELECTION, peer)
                i_am_biggest_bully = False
        if i_am_biggest_bully:
            self.declare_victory('no other bullies bigger than me')

    def declare_victory(self, reason):
        """
        Tell all the other members that I have decided that I am the biggest bully.

        :param reason: text to put out to the log as explanation
        """
        print('Victory by {} {}'.format(self.pid, reason))
        self.set_leader(self.pid)  # indicates election is over (note: I may not have known an election was happening)
        for member in self.members:
            if member != self.pid:
                peer = self.get_connection(member)
                if peer is None:
                    continue
                self.set_state(State.SEND_VICTORY, peer)
        self.set_quiescent()

    def update_members(self, their_idea_of_membership):
        """
        Pick up any new members from peer.

        :param their_idea_of_membership: the membership of group per the peer
        """
        if their_idea_of_membership is not None:
            for member in their_idea_of_membership:
                self.members[member] = their_idea_of_membership[member]

    @classmethod  # uses the class to know which receive method to call
    def send(cls, peer, message_name, message_data=None, wait_for_reply=False, buffer_size=BUF_SZ):
        """
        Pickles and sends the given message to the given socket and unpickles the returned value and returns it.

        :param peer: socket to send/recv
        :param message_name: text message name 'ELECTION', 'OK', etc.
        :param message_data: message contents (anything pickle-able)
        :param wait_for_reply: if a synchronous response is wanted
        :param buffer_size: if wait_for_reply, this is the receive buffer size
        :return: if wait_for_reply, the received response, otherwise None
        :raises: whatever pickle.dumps, socket.sendall, and receive could raise
        """
        message = message_name if message_data is None else (message_name, message_data)
        peer.sendall(pickle.dumps(message))
        if wait_for_reply:
            return cls.receive(peer, buffer_size)

    @staticmethod
    def receive(peer, buffer_size=BUF_SZ):
        """
        Receives and unpickles an incoming message from the given socket.

        :param peer: socket to recv from
        :param buffer_size: buffer size of socket.recv
        :return: the unpickled data received from peer
        :raises: whatever socket.recv or pickle.loads could raise
        """
        packet = peer.recv(buffer_size)
        if not packet:
            raise ValueError('socket closed')
        data = pickle.loads(packet)
        if type(data) == str:
            data = (data, None)  # turn a simple text message into a pair: (text, None)
        return data

    @staticmethod
    def start_a_server():
        """
        Start a socket bound to 'localhost' at a random port.

        :return: listening socket and its address
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(('localhost', 0))  # use any free socket
        listener.listen(BACKLOG)
        listener.setblocking(False)
        return listener, listener.getsockname()

    def join_group(self):
        """
        Join the group via the GCD.

        :raises TypeError: if we didn't get a reasonable response from GCD
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcd:
            message_data = (self.pid, self.listener_address)
            print('JOIN {}, {}'.format(self.gcd_address, message_data))
            gcd.connect(self.gcd_address)
            self.members = self.send(gcd, 'JOIN', message_data, wait_for_reply=True)
            if type(self.members) != dict:
                raise TypeError('got unexpected data from GCD: {}'.format(self.members))

    @staticmethod
    def pr_now():
        """Printing helper for current timestamp."""
        return datetime.now().strftime('%H:%M:%S.%f')

    def pr_sock(self, sock):
        """Printing helper for given socket."""
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    @staticmethod
    def cpr_sock(sock):
        """Static version of helper for printing given socket."""
        l_port = sock.getsockname()[1] % PEER_DIGITS
        try:
            r_port = sock.getpeername()[1] % PEER_DIGITS
        except OSError:
            r_port = '???'
        return '{}->{} ({})'.format(l_port, r_port, id(sock))

    def pr_leader(self):
        """Printing helper for current leader's name."""
        return 'unknown' if self.bully is None else ('self' if self.bully == self.pid else self.bully)


if __name__ == '__main__':
    if not 4 <= len(sys.argv) <= 5:
        print("Usage: python lab2.py GCDHOST GCDPORT SUID [DOB]")
        exit(1)
    if len(sys.argv) == 5:
        print('here one ')
        # assume ISO format for DOB, e.g., YYYY-MM-DD
        pieces = sys.argv[4].split('-')
        now = datetime.now()
        next_bd = datetime(now.year, int(pieces[1]), int(pieces[2]))
        if next_bd < now:
            next_bd = datetime(next_bd.year + 1, next_bd.month, next_bd.day)
    else:
        next_bd = datetime(2020, 1, 1)
    print('Next Birthday:', next_bd)
    su_id = int(sys.argv[3])
    print('SeattleU ID:', su_id)
    lab2 = Lab2(sys.argv[1:3], next_bd, su_id)
    lab2.run()
