require_relative 'concern/observable_shared'
require 'concurrent/atom'
require 'concurrent/atomic/count_down_latch'
require 'concurrent/atomic/cyclic_barrier'
require 'concurrent/atomic/atomic_fixnum'

module Concurrent

  RSpec.describe Atom do

    context 'construction' do

      it 'sets the initial value to the given value' do
        atom = Atom.new(42)
        expect(atom.value).to eq 42
      end
    end

    context '#compare_and_set' do

      it 'sets the new value if the current value matches' do
        atom = Atom.new(42)
        atom.compare_and_set(42, :foo)
        expect(atom.value).to eq :foo
      end

      it 'returns true if the current value matches' do
        atom = Atom.new(42)
        expect(atom.compare_and_set(42, :foo)).to be true
      end

      it 'rejects the new value if the current value does not match' do
        atom = Atom.new(42)
        atom.compare_and_set(:foo, 'bar')
        expect(atom.value).to eq 42
      end

      it 'returns false if the current value does not match' do
        atom = Atom.new(42)
        expect(atom.compare_and_set(:foo, 'bar')).to be false
      end

      it 'rejects the new value if the validator returns false' do
        validator = ->(value){ false }
        atom = Atom.new(42, validator: validator)
        atom.compare_and_set(42, :foo)
        expect(atom.value).to eq 42
      end

      it 'rejects the new value if the validator raises an exception' do
        validator = ->(value){ raise StandardError }
        atom = Atom.new(42, validator: validator)
        atom.compare_and_set(42, :foo)
        expect(atom.value).to eq 42
      end

      it 'returns false if the validator returns false' do
        validator = ->(value){ false }
        atom = Atom.new(42, validator: validator)
        expect(atom.compare_and_set(42, :foo)).to be false
      end

      it 'returns false if the validator raises an exception' do
        validator = ->(value){ raise StandardError }
        atom = Atom.new(42, validator: validator)
        expect(atom.compare_and_set(42, :foo)).to be false
      end
    end

    context '#swap' do

      it 'raises an exception when no block is given' do
        atom = Atom.new(42)
        expect {
          atom.swap
        }.to raise_error(ArgumentError)
      end

      it 'passes the current value to the block' do
        actual = nil
        expected = 42
        atom = Atom.new(expected)
        atom.swap do |value|
          actual = value
        end
        expect(actual).to eq expected
      end

      it 'passes all arguments to the block' do
        actual = nil
        expected = [1, 2, 3]
        atom = Atom.new(42)
        atom.swap(*expected) do |value, *args|
          actual = args
        end
        expect(actual).to eq expected
      end

      it 'sets the new value to the result of the block' do
        atom = Atom.new(42)
        atom.swap{ :foo }
        expect(atom.value).to eq :foo
      end

      it 'rejects the new value if the validator returns false' do
        validator = ->(value){ false }
        atom = Atom.new(42, validator: validator)
        atom.swap{ 100 }
        expect(atom.value).to eq 42
      end

      it 'rejects the new value if the validator raises an exception' do
        validator = ->(value){ raise StandardError }
        atom = Atom.new(42, validator: validator)
        atom.swap{ 100 }
        expect(atom.value).to eq 42
      end

      it 'returns the new value on success' do
        atom = Atom.new(42)
        expect(atom.swap{ :foo }).to eq :foo
      end

      it 'returns the old value if the validator returns false' do
        validator = ->(value){ false }
        atom = Atom.new(42, validator: validator)
        expect(atom.swap{ 100 }).to eq 42
      end

      it 'returns the old value if the validator raises an exception' do
        validator = ->(value){ raise StandardError }
        atom = Atom.new(42, validator: validator)
        expect(atom.swap{ 100 }).to eq 42
      end

      it 'calls the block more than once if the value changes underneath' do
        latch1 = Concurrent::CountDownLatch.new
        latch2 = Concurrent::CountDownLatch.new
        counter = Concurrent::AtomicFixnum.new(0)
        atom = Atom.new(0)

        t = in_thread do
          atom.swap do |value|
            latch1.count_down
            latch2.wait(1)
            counter.increment
            42
          end
        end

        latch1.wait(1)
        atom.swap{ 100 }
        latch2.count_down
        t.join(1)

        expect(counter.value).to be > 1
      end

      it 'reraises the exception from block' do
        atom = Atom.new(0)
        expect do
          atom.swap do |value|
            fail 'something went wrong'
          end
        end.to raise_error 'something went wrong'
      end
    end

    context '#reset' do

      it 'sets the new value' do
        atom = Atom.new(42)
        atom.reset(:foo)
        expect(atom.value).to eq :foo
      end

      it 'returns the new value on success' do
        atom = Atom.new(42)
        expect(atom.reset(:foo)).to eq :foo
      end

      it 'returns the new value on success' do
        atom = Atom.new(42)
        expect(atom.reset(:foo)).to eq :foo
      end

      it 'returns the old value if the validator returns false' do
        validator = ->(value){ false }
        atom = Atom.new(42, validator: validator)
        expect(atom.reset(:foo)).to eq 42
      end

      it 'returns the old value if the validator raises an exception' do
        validator = ->(value){ raise StandardError }
        atom = Atom.new(42, validator: validator)
        expect(atom.reset(:foo)).to eq 42
      end
    end

    context :observable do

      subject { Atom.new(0) }

      def trigger_observable(observable)
        observable.reset(42)
      end

      it_behaves_like :observable
    end

    context '#version and #snapshot' do

      it 'starts the version at zero' do
        atom = Atom.new(42)
        expect(atom.version).to eq 0
      end

      it 'reads the value and the version together in a snapshot' do
        atom = Atom.new(42)
        snapshot = atom.snapshot
        expect(snapshot).to be_a Atom::Snapshot
        expect(snapshot.value).to eq 42
        expect(snapshot.version).to eq 0
      end

      it 'supports array destructuring of the snapshot' do
        atom = Atom.new(42)
        value, version = atom.snapshot.to_a
        expect(value).to eq 42
        expect(version).to eq 0
      end

      it 'bumps the version on a successful compare_and_set' do
        atom = Atom.new(42)
        atom.compare_and_set(42, :foo)
        expect(atom.version).to eq 1
        expect(atom.snapshot.version).to eq 1
      end

      it 'bumps the version on a successful swap' do
        atom = Atom.new(42)
        atom.swap { |v| v + 1 }
        expect(atom.version).to eq 1
      end

      it 'bumps the version on a successful reset' do
        atom = Atom.new(42)
        atom.reset(:foo)
        expect(atom.version).to eq 1
      end

      it 'bumps the version even when the committed value is equal' do
        atom = Atom.new(42)
        atom.reset(42)
        expect(atom.value).to eq 42
        expect(atom.version).to eq 1

        atom.compare_and_set(42, 42)
        expect(atom.value).to eq 42
        expect(atom.version).to eq 2
      end

      it 'does not bump the version when the validator rejects' do
        validator = ->(value) { false }
        atom = Atom.new(42, validator: validator)
        atom.compare_and_set(42, :foo)
        atom.reset(:foo)
        atom.swap { :foo }
        expect(atom.version).to eq 0
        expect(atom.value).to eq 42
      end

      it 'does not bump the version when compare_and_set fails' do
        atom = Atom.new(42)
        atom.compare_and_set(:bogus, :foo)
        expect(atom.version).to eq 0
        expect(atom.value).to eq 42
      end

      it 'bumps the version once per successful commit' do
        atom = Atom.new(0)
        5.times { |i| atom.reset(i) }
        expect(atom.version).to eq 5
        expect(atom.snapshot.value).to eq 4
      end

      it 'detects an A to B to A change from versions even when values match' do
        atom = Atom.new(:a)
        first = atom.snapshot
        atom.reset(:b)
        atom.reset(:a)
        last = atom.snapshot

        expect(last.value).to eq first.value
        expect(last.version).to be > first.version
        expect(first).not_to eq last
      end
    end

    context '#wait_for_change' do

      it 'returns immediately with the current snapshot when already past the version' do
        atom = Atom.new(0)
        atom.reset(1)

        snapshot = atom.wait_for_change(0, 1)
        expect(snapshot).to be_a Atom::Snapshot
        expect(snapshot.version).to eq atom.version
        expect(snapshot.value).to eq 1
      end

      it 'treats a version lower than the initial version as already expired' do
        atom = Atom.new(0)
        snapshot = atom.wait_for_change(-5, 1)
        expect(snapshot.version).to eq 0
      end

      it 'returns nil on timeout without changing the atom' do
        atom = Atom.new(42)
        before = atom.snapshot

        started_at = Concurrent.monotonic_time
        snapshot = atom.wait_for_change(before.version, 0.1)
        elapsed = Concurrent.monotonic_time - started_at

        expect(snapshot).to be_nil
        expect(elapsed).to be >= 0.1
        expect(atom.snapshot).to eq before
      end

      it 'waits until a later commit and returns that snapshot' do
        atom = Atom.new(0)
        snapshot_queue = Queue.new

        waiter = in_thread do
          snapshot_queue << atom.wait_for_change(0, 2)
        end

        Thread.pass until waiter.status == 'sleep'
        atom.reset(:later)

        snapshot = snapshot_queue.pop
        expect(snapshot.value).to eq :later
        expect(snapshot.version).to eq 1
        waiter.join(1)
      end

      it 'wakes on a commit that leaves the value equal' do
        atom = Atom.new(:same)
        snapshot_queue = Queue.new

        waiter = in_thread do
          snapshot_queue << atom.wait_for_change(0, 2)
        end

        Thread.pass until waiter.status == 'sleep'
        atom.reset(:same)

        snapshot = snapshot_queue.pop
        expect(snapshot.value).to eq :same
        expect(snapshot.version).to eq 1
        waiter.join(1)
      end

      it 'does not wake on a failed compare_and_set' do
        atom = Atom.new(0)
        snapshot_queue = Queue.new

        waiter = in_thread do
          snapshot_queue << atom.wait_for_change(0, 0.2)
        end

        Thread.pass until waiter.status == 'sleep'
        atom.compare_and_set(:bogus, 1)

        expect(snapshot_queue.pop).to be_nil
        expect(atom.version).to eq 0
        waiter.join(1)
      end

      it 'does not wake when the validator rejects the new value' do
        atom = Atom.new(0, validator: ->(v) { v != 1 })
        snapshot_queue = Queue.new

        waiter = in_thread do
          snapshot_queue << atom.wait_for_change(0, 0.2)
        end

        Thread.pass until waiter.status == 'sleep'
        atom.compare_and_set(0, 1)

        expect(snapshot_queue.pop).to be_nil
        expect(atom.version).to eq 0
        waiter.join(1)
      end

      it 'lets every registered waiter observe the same commit' do
        atom = Atom.new(0)
        waiter_count = 4
        registered = Concurrent::CyclicBarrier.new(waiter_count + 1)
        results = Array.new(waiter_count) { Queue.new }

        waiters = waiter_count.times.map do |i|
          in_thread do
            registered.wait(2)
            results[i] << atom.wait_for_change(0, 3)
          end
        end

        expect(registered.wait(2)).to be true
        atom.reset(42)

        waiters.each_with_index do |waiter, i|
          snapshot = results[i].pop
          expect(snapshot.version).to eq 1
          expect(snapshot.value).to eq 42
          waiter.join(1)
        end
      end

      it 'is not required to preserve intermediate values' do
        atom = Atom.new(0)
        snapshot_queue = Queue.new

        waiter = in_thread do
          snapshot_queue << atom.wait_for_change(0, 2)
        end

        Thread.pass until waiter.status == 'sleep'
        atom.reset(1)
        atom.reset(2)
        atom.reset(3)

        snapshot = snapshot_queue.pop
        expect(snapshot.version).to eq 3
        expect(snapshot.value).to eq 3
        waiter.join(1)
      end

      it 'chains waits and observes every committed version' do
        atom = Atom.new(0)
        observed = Queue.new

        waiter = in_thread do
          version = atom.snapshot.version
          3.times do
            snapshot = atom.wait_for_change(version, 2)
            observed << snapshot.version
            version = snapshot.version
          end
        end

        1.upto(3) do |i|
          Thread.pass until waiter.status == 'sleep'
          atom.reset(i)
        end

        waiter.join(2)
        expect(observed.length).to eq 3
        expect(3.times.map { observed.pop }).to eq [1, 2, 3]
      end

      it 'never loses a commit when the snapshot is read concurrently with it' do
        atom = Atom.new(0)
        ready = Concurrent::CyclicBarrier.new(2)
        result = Queue.new

        reader = in_thread do
          ready.wait(2)
          snapshot = atom.snapshot
          newer = atom.wait_for_change(snapshot.version, 2)
          result << [snapshot.version, newer]
        end

        committer = in_thread do
          ready.wait(2)
          atom.reset(1)
        end

        reader.join(2)
        committer.join(2)

        observed_version, newer = result.pop
        expect(observed_version).to satisfy('see either side of the commit') { |v| v == 0 || v == 1 }
        if observed_version == 0
          expect(newer).not_to be_nil
          expect(newer.version).to be >= 1
        end
      end

      it 'handles a commit arriving at the timeout boundary without loss' do
        atom = Atom.new(0)
        outcomes = Queue.new
        threads = 8.times.map do
          in_thread do
            snapshot = atom.wait_for_change(0, 0.05)
            outcomes << (snapshot.nil? ? nil : snapshot.version)
          end
        end

        sleep 0.05
        atom.reset(1)
        threads.each { |t| t.join(2) }

        versions = 8.times.map { outcomes.pop }
        expect(versions).to all(satisfy('be a timeout or the committed version') { |v| v.nil? || v >= 1 })
        expect(atom.version).to eq 1
      end
    end

    context 'observation with versions' do

      it 'notifies observers on every successful commit' do
        atom = Atom.new(0)
        events = Queue.new
        atom.add_observer { |time, old_value, new_value| events << [old_value, new_value] }

        atom.reset(1)
        atom.compare_and_set(1, 1)

        expect(events.length).to eq 2
      end

      it 'does not notify on rejected validation or failed CAS' do
        atom = Atom.new(0, validator: ->(v) { v != 99 })
        events = Queue.new
        atom.add_observer { |*args| events << args }

        atom.compare_and_set(:bogus, 1)
        atom.compare_and_set(0, 99)
        atom.reset(99)
        atom.swap { 99 }

        expect(events).to be_empty
        expect(atom.version).to eq 0
      end

      it 'allows an observer to mutate the atom from its callback without deadlock' do
        atom = Atom.new(0)
        done = Concurrent::CountDownLatch.new

        atom.add_observer do |_time, _old_value, new_value|
          atom.reset(new_value + 100) if new_value < 100
          done.count_down if new_value >= 100
        end

        atom.reset(1)
        expect(done.wait(2)).to be true
        expect(atom.value).to eq 101
      end

      it 'passes each notification the values captured for its own commit' do
        atom = Atom.new(0)
        seen = Queue.new
        inside = Concurrent::CountDownLatch.new
        release = Concurrent::CountDownLatch.new
        second_started = Concurrent::CountDownLatch.new

        observer = proc do |_time, old_value, new_value|
          if new_value == 1
            inside.count_down
            release.wait(2)
          elsif new_value == 2
            second_started.count_down
          end
          seen << [old_value, new_value]
        end
        atom.add_observer(&observer)

        first = in_thread { atom.reset(1) }
        expect(inside.wait(2)).to be true

        second = in_thread { atom.reset(2) }
        expect(second_started.wait(2)).to be true

        release.count_down
        first.join(2)
        second.join(2)
        atom.delete_observer(observer)

        notifications = 2.times.map { seen.pop }.sort_by(&:last)
        expect(notifications).to eq [[0, 1], [1, 2]]
      end
    end
  end
end
