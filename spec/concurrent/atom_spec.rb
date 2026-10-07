require_relative 'concern/observable_shared'
require 'concurrent/atom'
require 'concurrent/atomic/count_down_latch'
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

    context '#snapshot' do

      it 'returns the initial value with version zero' do
        snapshot = Atom.new(42).snapshot
        expect(snapshot.value).to eq 42
        expect(snapshot.version).to eq 0
      end

      it 'reads the value and the version together' do
        atom = Atom.new(0)
        3.times { |i| atom.reset(i + 1) }
        snapshot = atom.snapshot
        expect(snapshot.value).to eq 3
        expect(snapshot.version).to eq 3
      end
    end

    context 'versioning' do

      it 'advances the version on every successful commit' do
        atom = Atom.new(0)
        expect { atom.swap { |v| v + 1 } }.to change { atom.snapshot.version }.by(1)
        expect { atom.compare_and_set(1, 2) }.to change { atom.snapshot.version }.by(1)
        expect { atom.reset(3) }.to change { atom.snapshot.version }.by(1)
      end

      it 'advances the version even when the new value equals the old value' do
        atom = Atom.new(42)
        expect { atom.swap { |v| v } }.to change { atom.snapshot.version }.by(1)
        expect { atom.reset(42) }.to change { atom.snapshot.version }.by(1)
        expect { atom.compare_and_set(42, 42) }.to change { atom.snapshot.version }.by(1)
      end

      it 'does not advance the version when the validator rejects the value' do
        atom = Atom.new(42, validator: ->(v) { v != :bad })
        expect { atom.swap { :bad } }.not_to change { atom.snapshot.version }
        expect { atom.reset(:bad) }.not_to change { atom.snapshot.version }
        expect { atom.compare_and_set(42, :bad) }.not_to change { atom.snapshot.version }
      end

      it 'does not advance the version when compare_and_set does not match' do
        atom = Atom.new(42)
        expect { atom.compare_and_set(:other, 100) }.not_to change { atom.snapshot.version }
      end
    end

    context '#wait_for_change' do

      it 'returns the current snapshot immediately when the version is stale' do
        atom = Atom.new(0)
        atom.reset(:newer)
        snapshot = atom.wait_for_change(0, 0.1)
        expect(snapshot.value).to eq :newer
        expect(snapshot.version).to eq 1
      end

      it 'blocks until a commit advances the version' do
        atom = Atom.new(0)
        before = atom.snapshot
        waiter = in_thread { atom.wait_for_change(before.version, 5) }
        is_sleeping(waiter)
        atom.reset(:changed)
        snapshot = waiter.value
        expect(snapshot.value).to eq :changed
        expect(snapshot.version).to eq before.version + 1
      end

      it 'detects an A->B->A sequence through the version' do
        atom = Atom.new(:a)
        before = atom.snapshot
        waiter = in_thread { atom.wait_for_change(before.version, 5) }
        is_sleeping(waiter)
        atom.reset(:b)
        atom.reset(:a)
        snapshot = waiter.value
        expect(snapshot.value).to eq :a
        expect(snapshot.version).to eq before.version + 2
      end

      it 'wakes when a commit stores an equal value' do
        atom = Atom.new(42)
        before = atom.snapshot
        waiter = in_thread { atom.wait_for_change(before.version, 5) }
        is_sleeping(waiter)
        atom.reset(42)
        snapshot = waiter.value
        expect(snapshot.value).to eq 42
        expect(snapshot.version).to eq before.version + 1
      end

      it 'ends the wait on timeout without modifying the atom' do
        atom = Atom.new(42)
        before = atom.snapshot
        snapshot = atom.wait_for_change(before.version, 0.1)
        expect(snapshot.value).to eq 42
        expect(snapshot.version).to eq before.version
        expect(atom.value).to eq 42
        expect(atom.snapshot.version).to eq before.version
      end

      it 'returns the committed snapshot when an update arrives before the timeout' do
        atom = Atom.new(0)
        before = atom.snapshot
        waiter = in_thread { atom.wait_for_change(before.version, 5) }
        is_sleeping(waiter)
        sleep 0.1
        atom.reset(:updated)
        snapshot = waiter.value
        expect(snapshot.value).to eq :updated
        expect(snapshot.version).to eq before.version + 1
      end

      it 'times out when the update arrives only after the timeout' do
        atom = Atom.new(0)
        before = atom.snapshot
        waiter = in_thread { atom.wait_for_change(before.version, 0.2) }
        is_sleeping(waiter)
        snapshot = waiter.value
        expect(snapshot.version).to eq before.version
        atom.reset(:too_late)
        expect(atom.snapshot.version).to eq before.version + 1
      end

      it 'wakes multiple waiters on the same commit' do
        atom = Atom.new(0)
        before = atom.snapshot
        waiters = 3.times.map { in_thread { atom.wait_for_change(before.version, 5) } }
        waiters.each { |waiter| is_sleeping(waiter) }
        atom.reset(:shared)
        waiters.each do |waiter|
          snapshot = waiter.value
          expect(snapshot.value).to eq :shared
          expect(snapshot.version).to eq before.version + 1
        end
      end

      it 'does not deadlock or lose versions when an observer updates the atom' do
        atom = Atom.new(0)
        before = atom.snapshot
        atom.add_observer do |_, _, new_value|
          atom.reset(new_value + 100) if new_value < 100
        end
        atom.reset(1)
        expect(atom.value).to eq 101
        expect(atom.snapshot.version).to eq before.version + 2
      end
    end
  end
end
