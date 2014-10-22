
require_relative 'core'
require 'ttftree'

module STM
  class Empty < StandardError
  end
  
  class Full < StandardError
  end
end


class STM::TArray
  def initialize
    @var = STM::TVar.new(TTFTree::Empty.new(TTFTree::Measures::ITEM_COUNT))
  end
  
  def [](index)
    # TODO: Handle ranges and negative indexes
    left, right = @var.get.partition { |v| v > index }
    right.empty? ? nil : right.first
  end
  
  def []=(index, value)
    left, right = @var.get.partition { |v| v > index }
    @var.set left.add_last(value).append(right.without_first)
  end
  
  def delete_at(index)
    left, right = @var.get.partition { |v| v > index }
    return nil if right.empty?
    @var.set left.append(right.without_first)
    right.first
  end
  
  def length
    @var.get.annotation
  end
  
  def insert(index, *values)
    left, right = @var.get.partition { |v| v > index }
    left.append(TTFTree.to_tree(@var.get.measure, values)).append(right)
  end
  
  def each
    tree = @var.get
    until tree.empty?
      yield tree.first
      tree = tree.without_first
    end
  end
  
  def to_a
    [*self]
  end
end


class STM::BroadcastQueue
  def initialize
    @next_var_container = STM::TVar.new STM::TVar.new(nil)
    @written = STM::TVar.new 0
  end
  
  def put(value)
    next_var = STM::TVar.new
    item = [value, next_var]
    @next_var_container.get.set item
    @next_var_container.set next_var
    @written.set @written.get + 1
  end
  
  def new_endpoint
    STM::BroadcastEndpoint.new(self, @next_var_container.get, @written.get)
  end
  
  # Internal: ...
  def written
    @written.get
  end
end


class STM::BroadcastEndpoint
  # Internal: ...
  def initialize(queue, var, read)
    @queue = queue
    @next_var_container = STM::TVar.new var
    @read = STM::TVar.new read
  end
  
  def get(block=true)
    if @next_var_container.get.get.nil?
      if block
        STM.try_later
      else
        raise Empty
      end
    else
      value, next_var = @next_var_container.get.get
      @next_var_container.set next_var
      @read.set @read.get + 1
      value
    end
  end
  
  def replace(value)
    @next_var_container.set STM::TVar.new([value, @next_var_container.get])
    @read.set @read.get - 1
  end
  
  def peek(block=false)
    value = get(block)
    replace(value)
    value
  end
  
  def empty?
    @next_var_container.get.get.nil?
  end
  
  def duplicate
    BroadcastEndpoint.new(@queue, @next_var_container.get, @read.get)
  end
  
  def remaining
    @queue.written - @read.get
  end
end

