
require_relative 'core'
require_relative 'datatypes'
require 'ttftree'

module STM
  class Empty < StandardError
  end

  class Full < StandardError
  end
end


class STM::TArray
  include STM::Helpers

  attr_transactional :tree

  def initialize
    self.tree = TTFTree::Empty.new(TTFTree::Measures::ITEM_COUNT)
  end

  def [](index)
    # TODO: Handle ranges and negative indexes
    left, right = tree.partition { |v| v > index }
    right.empty? ? nil : right.first
  end

  def []=(index, value)
    left, right = tree.partition { |v| v > index }
    tree = left.add_last(value).append(right.without_first)
  end

  def delete_at(index)
    left, right = tree.partition { |v| v > index }
    return nil if right.empty?
    tree = left.append(right.without_first)
    right.first
  end

  def length
    tree.annotation
  end

  def insert(index, *values)
    left, right = tree.partition { |v| v > index }
    tree = left.append(TTFTree.to_tree(tree.measure, values)).append(right)
  end

  def each
    current_tree = tree
    until current_tree.empty?
      yield current_tree.first
      current_tree = current_tree.without_first
    end
  end

  def to_a
    [*self]
  end
end


class STM::BroadcastQueue
  include STM::Helpers

  attr_transactional :next_var, :written

  def initialize
    self.next_var = tvar(nil)
    self.written = 0
  end

  def put(value)
    v = tvar(nil)
    item = [value, v]
    self.next_var.set item
    self.next_var = v
    self.written += 1
  end

  def new_endpoint
    STM::BroadcastEndpoint.new(self, next_var, written)
  end
end


class STM::BroadcastEndpoint
  include STM::Helpers

  attr_transactional :next_var, :read

  def initialize(queue, var, read)
    @queue = queue
    self.next_var = var
    self.read = read
  end

  def get(block=true)
    if next_var.get.nil?
      if block
        try_later
      else
        raise Empty
      end
    else
      value, v = next_var.get
      self.next_var = v
      self.read += 1
      value
    end
  end

  def replace(value)
    self.next_var = tvar([value, self.next_var])
    self.read -= 1
  end

  def peek(block=false)
    value = get(block)
    replace(value)
    value
  end

  def empty?
    next_var.get.nil?
  end

  def dup
    BroadcastEndpoint.new(@queue, next_var, read)
  end

  def remaining
    @queue.written - read
  end
end

