
require_relative 'core'
require 'ttftree'

module STM
end


class STM::TValueIterator


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

