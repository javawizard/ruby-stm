
require 'singleton'

module TTFTree
  class TreeIsEmpty < StandardError
    def initialize
      super "This tree is empty"
    end
  end

  class Identity
    include Singleton
  end
  
  IDENTITY = Identity.instance
  
  class Measure
    attr_reader :identity
    
    def initialize(convert, operator, identity)
      @convert = convert
      @operator = operator
      @identity = identity
    end
      
    def convert(value)
      @convert.call(value)
    end
    
    def operator(a, b)
      @operator.call(a, b)
    end
    
    def convert_function
      @convert
    end
    
    def operator_function
      @operator
    end
  end
  
  module Measures
    def self.item_count
      # Probably ought to make this a constant
      Measure.new ->(v){ 1 }, ->(a, b){ a + b }, 0
    end
    
    def self.from_semigroup(convert, operator)
      Measure.new convert, ->(a, b){
        if a == IDENTITY
          b
        elsif b == IDENTITY
          a
        else
          operator(a, b)
        end
      }, IDENTITY 
    end
    
    def self.last_item
      from_semigroup ->(v){ v }, ->(a, b){ b }
    end
    
    def self.min_max
      from_semigroup ->(v){ v }, ->(a, b){ [[a, b].min, [a, b].max] }
    end
    
    def self.translate measure
      Measure.new ->(v){
        yield v
      }, measure.convert_function, measure.operator_function
    end
    
    def self.compound *measures
      Measure.new ->(v){
        measures.collect { |measure| measure.convert v }
      }, ->(a, b){
        measures.collect { |measure| measure.operator a, b }
      }, measures.collect { |measure| measure.identity }
    end
  end
  
  class Node
    attr_reader :annotation, :values
    
    def initialize(measure, *values)
      @values = values
      @measure = measure
      @annotation = values.collect(&measure.convert_function).reduce(@measure.identity, &measure.operator_function)
    end
    
    def [](index)
      # Is there a better way to do this in Ruby?
      if index.is_a? Range
        Node.new @measure, *@values[index]
      else
        @values[index]
      end
    end
    
    def length
      @values.length
    end
    
    def partition_node(initial_annotation)
      split_point = 0
      while split_point < length
        current_annotation = @measure.operator(initial_annotation, @measure.convert(self[split_point]))
        if yield current_annotation
          break
        else
          split_point += 1
          initial_annotation = current_annotation
        end
      end
      [self[0...split_point], self[split_point..-1]]
    end
    
    def inspect
      "<Node: #{@values.collect(&:inspect).join ', '}>"
    end
  end
  
  def self.to_tree(measure, list)
    tree = Empty.new measure
    list.each do |value|
      tree = tree.add_last value
    end
    tree
  end
  
  class Tree
    attr_reader :annotation
    
    def initialize(measure, annotation)
      @measure = measure
      @annotation = annotation
    end
    
    def partition(&predicate)
      partition_with(@measure.identity, &predicate)
    end
  end
  
  class Empty < Tree
    def initialize measure
      super measure, measure.identity
    end
    
    def empty?
      true
    end
    
    def first
      raise TreeIsEmpty
    end
    
    def without_first
      raise TreeIsEmpty
    end
    
    def add_first(item)
      Single.new(@measure, item)
    end
    
    def last
      raise TreeIsEmpty
    end
    
    def without_last
      raise TreeIsEmpty
    end
    
    def add_last(item)
      Single.new(@measure, item)
    end
    
    def prepend(other)
      other
    end
    
    def append(other)
      other
    end
    
    def partition_with(initial_annotation)
      [self, self]
    end
    
    def inspect
      "<Empty>"
    end
  end
  
  class Single < Tree  
    private def node_measure measure
      Measure.new ->(node){ node.annotation }, measure.operator_function, measure.identity
    end

    def initialize(measure, item)
      super measure, measure.convert(item)
      @item = item
    end
    
    def empty?
      false
    end
    
    def first
      @item
    end
    
    def without_first
      Empty.new @measure
    end
    
    def add_first item
      Deep.new @measure, Node.new(@measure, item), Empty.new(node_measure @measure), Node.new(@measure, @item)
    end
    
    def last
      @item
    end
    
    def without_last
      Empty.new @measure
    end
    
    def add_last item
      Deep.new @measure, Node.new(@measure, @item), Empty.new(node_measure @measure), Node.new(@measure, item)
    end
    
    def prepend(other)
      other.add_last(@item)
    end
    
    def append(other)
      other.add_first(@item)
    end
    
    def partition_with(initial_annotation)
      if yield @measure.operator(initial_annotation, @annotation)
        [Empty.new(@measure), self]
      else
        [self, Empty.new(@measure)]
      end
    end
    
    def inspect
      "<Single: #{@item.inspect}>"
    end
  end
  
  class Deep < Tree
    attr_reader :left, :spine, :right
    protected :left, :spine, :right
  
    def initialize(measure, left, spine, right)
      super measure, measure.operator(measure.operator(left.annotation, spine.annotation), right.annotation)
      @left = left
      @spine = spine
      @right = right
    end
    
    def empty?
      false
    end
    
    def first
      left[0]
    end
    
    def without_first
      if @left.length > 1
        Deep.new @measure, @left[1..-1], @spine, @right
      elsif !@spine.empty?
        Deep.new @measure, @spine.first, @spine.without_first, @right
      elsif @right.length == 1
        Single.new @measure, @right[0]
      else
        Deep.new @measure, @right[0..0], @spine, @right[1..-1]
      end
    end
    
    def add_first item
      if @left.length < 4
        Deep.new @measure, Node.new(@measure, *[item] + @left.values), @spine, @right
      else
        node = Node.new @measure, *@left.values[1..3]
        new_left = Node.new @measure, item, @left[0]
        Deep.new @measure, new_left, @spine.add_first(node), @right
      end
    end
    
    def last
      @right[-1]
    end
    
    def without_last
      if @right.length > 1
        Deep.new @measure, @left, @spine, @right[0..-2]
      elsif !@spine.empty?
        Deep.new @measure, @left, @spine.without_last, @spine.last
      elsif @left.length == 1
        Single.new @measure, @left[0]
      else
        Deep.new @measure, @left[0..-2], @spine, @left[-1]
      end
    end
    
    def add_last item
      if @right.length < 4
        Deep.new @measure, @left, @spine, Node.new(@measure, *@right.values + [item])
      else
        node = Node.new @measure, *@right.values[0..2]
        new_right = Node.new @measure, @right[3], item
        Deep.new @measure, @left, @spine.add_last(node), new_right
      end
    end
    
    def prepend(other)
      other.append self
    end
    
    def append(other)
      if other.is_a? Deep
        Deep.new @measure, @left, fold_up(other), other.right
      else
        other.prepend self
      end
    end
    
    private def fold_up(other)
      middle_items = self.right.values + other.left.values
      spine = self.spine
      
      until middle_items.empty?
        if middle_items.length == 2 || middle_items.length || 4
          spine = spine.add_last Node.new @measure, *middle_items[0..1]
          middle_items.slice! 0..1
        else
          spine = spine.add_last Node.new @measure, *middle_items[0..2]
          middle_items.slice! 0..2
        end
      end
      
      spine.append(other.spine)
    end
    
    def partition_with(initial_annotation, &predicate)
      left_annotation = @measure.operator(initial_annotation, @left.annotation)
      spine_annotation = @measure.operator(left_annotation, @spine.annotation)
      if predicate.call left_annotation
        left_items, right_items = @left.partition_node(initial_annotation, &predicate)
        [TTFTree::to_tree(@measure, left_items.values), deep_left(right_items, @spine, @right)]
      elsif predicate.call spine_annotation
        left_spine, right_spine = @spine.partition_with(left_annotation, &predicate)
        split_node = right_spine.first
        right_spine = right_spine.without_first
        before_items, after_items = split_node.partition_node(@measure.operator(left_annotation, left_spine.annotation), &predicate)
        [deep_right(@left, left_spine, before_items), deep_left(after_items, right_spine, @right)]
      else
        left_items, right_items = @right.partition_node(spine_annotation, &predicate)
        [deep_right(@left, @spine, left_items), TTFTree::to_tree(@measure, right_items.values)]
      end
    end
    
    def inspect
      "<Deep left: #{@left.values.inspect}, spine: #{@spine.inspect}, right: #{@right.values.inspect}>"
    end
    
    private
    
    def deep_left(maybe_left, spine, right)
      if maybe_left.length != 0
        Deep.new(@measure, maybe_left, spine, right)
      elsif spine.empty?
        TTFTree::to_tree(@measure, right.values)
      else
        Deep.new(@measure, spine.first, spine.without_first, right)
      end
    end
    
    def deep_right(left, spine, maybe_right)
      if maybe_right.length != 0
        Deep.new(@measure, left, spine, maybe_right)
      elsif spine.empty?
        TTFTree::to_tree(@measure, left.values)
      else
        Deep.new(@measure, left, spine.without_last, spine.last)
      end
    end
  end
end





