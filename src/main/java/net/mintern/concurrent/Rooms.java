package net.mintern.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class provides {@link Object#wait()}-free, lock-free synchronization that allows exclusive
 * access to logical groups of operations, which we call <i>rooms</i>. It implements the algorithm
 * described in <a href="https://www.cs.cmu.edu/~blelloch/papers/BCG03.pdf">Scalable Room
 * Synchronizations</a>. Multiple threads can simultaneously enter the same room, but threads cannot
 * simultaneously enter different rooms.
 * <p>
 * For example, consider a pool of workers that update metrics after performing their work.
 * Separately, monitoring code compiles and reports those metrics on-demand. If atomic operations
 * are used, workers can update the metrics simultaneously without losing information. Monitors can
 * also compile metrics at the same time, provided a monitor isn't reading values while a worker is
 * is in the process of updating them. Using {@code Rooms}, that pattern can be implemented as
 * follows:
 * <p>
 * <pre>{@code
 *  final Rooms metrics = new Rooms(2);
 *  final AtomicInteger workUnits = new AtomicInteger();
 *  final AtomicLong totalTime = new AtomicLong();
 *
 *  private void update(long timeElapsed) {
 *      try (Room update = metrics.enter(0)) {
 *          workUnits.incrementAndGet();
 *          totalTime.addAndGet(timeElapsed);
 *      }
 *  }
 *
 *  public double timePerUnit() {
 *      int units;
 *      long time;
 *      try (Room compile = metrics.enter(1)) {
 *          units = workUnits.get();
 *          time = totalTime.get();
 *      }
 *      return (double) units / time;
 *  }
 * }</pre>
 * <p>
 * For more examples, refer to the paper.
 * <p>
 * A fixed number of rooms {@code n} is specified at construction time:
 * <p>
 * <pre>{@code
 *  Rooms rooms = new Rooms(n);
 * }</pre>
 * <p>
 * Thereafter, a code block can enter a room {@code i}, where {@code 0 <= i < n}, by calling:
 * <p>
 * <pre>{@code
 *  try (Room room_i = rooms.enter(i)) {
 *      // Perform the room operation.
 *  }
 * }</pre>
 * <p>
 * Alternatively, a <i>try-finally</i> can be used:
 * <p>
 * <pre>{@code
 *  Room room_i = rooms.enter(i);
 *  try {
 *      // Perform the room operation.
 *  } finally {
 *      room_i.exit();
 *  }
 * }</pre>
 * <p>
 * <b>It is very important that <i>either</i> {@link Room#exit()} <i>or</i> {@link Room#close()} is
 * called <i>exactly once</i> for each call to {@link #enter(int)}.
 *
 * @author Brandon Mintern
 */
public class Rooms {

    private static final boolean DEBUG = true;

    private final Room[] rooms;

    /**
     * True iff any room is currently active.
     */
    private final AtomicBoolean active = new AtomicBoolean();

    /**
     * The index of the currently-active room, meaningless when {@link #active} is {@code false}.
     */
    private volatile int activeRoom;

    /**
     * Creates a group of {@code n} rooms. {@link #enter(int)} can be called with values {@code i}
     * where {@code 0 <= i < n}.
      *
     * @param n  the number of rooms
     */
    public Rooms(int n) {
        rooms = new Room[n];
        for (int i = 0; i < n; i++) {
            rooms[i] = new Room();
        }
    }

    /**
     * Spins until {@code room} can be entered. Intended to be used in a try-with-resources block:
     * <pre>{@code
     *  try (Room room = myRooms.enter(0)) {
     *      // Perform activity while no other rooms can be entered.
     *      // The `room` should not be used directly.
     *  }
     *  // other rooms can now be entered
     * }</pre>
     * <p>
     * If a try-with-resources cannot be used, then {@link Room#exit()} can be called directly.
     * <p>
     * <b>It is very important that either {@code exit} or {@code close} is called exactly once for
     * each call to {@code enter}!</b> If you are using a try-with-resources, then
     * {@link Room#close()} will be called automatically.
     *
     * @param room  the room to enter, must be less than the number passed to the constructor
     * @return the room that was entered
     * @throws IndexOutOfBoundsException if {@code room} is outside the bounds of {@code this}
     */
    public Room enter(int room) {
        Room r = rooms[room];
        long myTicket = r.entries.incrementAndGet();    // get ticket for the room
        while (myTicket > r.grant) {                    // wait until ticket is granted
            if (active.compareAndSet(false, true)) {    // while waiting, if no active room
                activeRoom = room;                      // then make `room` the active room
                r.grant = r.entries.get();              // grant tickets to enter room `room`
                return r;
            }
        }
        return r;
    }

    /**
     * The room that has been entered. The only meaningful operation is to {@link #exit()} the room.
     * All calls to {@link RoomLock#enter(int)} with the same value {@code i} will return the same
     * {@code Room} object. Users can {@code synchronize} on that object for more fine-grained
     * locking.
     */
    public class Room implements AutoCloseable {

        /**
         * Indicates the number of entries into this room. Each attempt to enter the room grabs a
         * new {@code entries} value (which we call a <i>ticket</i>). See {@link #grant}.
         */
        private final AtomicLong entries = new AtomicLong();

        /**
         * All tickets (see {@link #entries}) higher than {@code grant} are waiting to execute. When
         * {@link #exits} {@code < grant}, this room is still executing.
         */
        private volatile long grant;

        /**
         * Indicates the number of exits from this room.
         */
        private final AtomicLong exits = new AtomicLong();

        /**
         * Exits this room. <b>This method (or {@link #exit()}) must be called exactly once!</b>.
         */
        @Override
        public void close() {
            exit();
        }

        /**
         * Exits this room. <b>This method (or {@link #close()}) must be called exactly once!</b>.
         */
        public void exit() {
            if (DEBUG) {
                // We can't catch every problem, but this will help sometimes.
                if (!active.get() || this != rooms[activeRoom] || exits.get() >= grant) {
                    throw new IllegalStateException("exit/close called more than once, possibly elsewhere");
                }
            }
            // If we are the last of this batch to exit, activate the next room.
            if (exits.incrementAndGet() == grant) {
                setNextActiveRoom(activeRoom + 1); // our room will be tried last
            }
        }
    }

    private void setNextActiveRoom(int nextRoom) {
        // Check each room, starting with nextRoom and wrapping around rooms. If there are ticketed
        // waiters for a room, then we make it active and grant entry to all of that room's tickets.
        for (int k = 0; k < rooms.length; k++) {
            int newActiveRoom = (nextRoom + k) % rooms.length;
            Room nar = rooms[newActiveRoom];
            long currWait = nar.entries.get();
            if (currWait > nar.grant) {
                activeRoom = newActiveRoom;
                nar.grant = currWait;
                return;
            }
        }
        // No waiters found, so no active room. The enter method will set the next active room.
        active.set(false);
    }

    /**
     * {@link Rooms} that operate on {@code enum} values instead of {@code int}s. When the set of
     * rooms is fixed, declaring them in an {@code enum} and then using this class may prove to be a
     * more robust, self-documenting alternative to using {@code Rooms} directly.
     *
     * @param <E> the enumerated type
     */
    public static class Enumerated<E extends Enum<E>> {

        private final Rooms rooms;

        /**
         * Creates a group of rooms where each of {@code enumCls}'s values has its own room.
         *
         * @param enumCls   the {@code enum} class
         */
        public Enumerated(Class<E> enumCls) {
            rooms = new Rooms(enumCls.getEnumConstants().length);
        }

        /**
         * Just like {@link Rooms#enter(int)}, except that it accepts an {@code E room} instead of
         * an {@code int}. Be sure to call {@link Room#exit()} or {@link Room#close()} exactly once.
         *
         * @param room  the room to enter
         * @return the room that was entered
         */
        public Room enter(E room) {
            return rooms.enter(room.ordinal());
        }
    }
}
