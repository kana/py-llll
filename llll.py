'''
py-llll - LINQ-like list processing library in Python
Copyright (C) 2011 Kana Natsuno <http://whileimautomaton.net/>
License: So-called MIT/X license
    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or
    sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following
    conditions:

    The above copyright notice and this permission notice shall be
    included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
    OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT.  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
    OTHER DEALINGS IN THE SOFTWARE.
'''

class Query:
  def __init__(self, ys_from_xs):
    self.ys_from_xs = ys_from_xs
    return

  def __ror__(self, xs):
    return self.ys_from_xs(xs)

def queryize(original_query):
  def wrapped_query(*args, **kw):
    return Query(lambda xs: original_query(xs, *args, **kw))
  wrapped_query.__doc__ = original_query.__doc__
  return wrapped_query

class OrderedSequence:
  def __init__(self, xs, key_from_x):
    self.xs = xs
    self.key_from_x = key_from_x
    return

  def __iter__(self):
    xsd = self.xs | to_list()
    xsd.sort(key = self.key_from_x)
    for x in xsd:
      yield x




@queryize
def all(xs, predicate):
  '''
  >>> [1, 2, 3] | all(lambda x: isinstance(x, int))
  True
  >>> [1, 'b', 3] | all(lambda x: isinstance(x, int))
  False
  >>> ['a', 'b', 'c'] | all(lambda x: isinstance(x, int))
  False
  >>> [] | all(lambda x: isinstance(x, int))
  True
  '''
  for x in xs:
    if not predicate(x):
      return False
  return True

@queryize
def any(xs, predicate = lambda x: True):
  '''
  >>> [1, 2, 3] | any(lambda x: isinstance(x, int))
  True
  >>> [1, 'b', 3] | any(lambda x: isinstance(x, int))
  True
  >>> ['a', 'b', 'c'] | any(lambda x: isinstance(x, int))
  False
  >>> [] | any(lambda x: isinstance(x, int))
  False

  >>> [1, 2, 3] | any()
  True
  >>> [] | any()
  False
  '''
  for x in xs:
    if predicate(x):
      return True
  return False

@queryize
def average(xs, number_from_x = lambda x: x):
  '''
  >>> [1, 2, 3] | average()
  2
  >>> ['a', 'ab', 'abc'] | average(lambda x: len(x))
  2
  >>> [] | average()
  Traceback (most recent call last):
    ...
  ValueError: Sequence must contain some element
  '''
  sum = 0
  count = 0
  for x in xs:
    sum += number_from_x(x)
    count += 1
  if count == 0:
    raise ValueError('Sequence must contain some element')
  return sum / count

@queryize
def concat(xs, ys):
  '''
  >>> xs = repeat(1, 3)
  >>> ys = repeat(2, 3)
  >>> xs | concat(ys) | to_tuple()
  (1, 1, 1, 2, 2, 2)
  '''
  for x in xs:
    yield x
  for y in ys:
    yield y

@queryize
def contains(xs, the_x):
  '''
  >>> [1, 2, 3] | contains(2)
  True
  >>> [1, 2, 3] | contains('2')
  False
  >>> [] | contains(2)
  False
  '''
  return xs | any(lambda x: x == the_x)

@queryize
def count(xs, predicate = lambda x: True):
  '''
  >>> [1, 2, 3] | count()
  3
  >>> range(10) | where(lambda x: x % 2 == 0) | count()
  5
  >>> [] | count()
  0
  '''
  return __builtins__.sum(1 for x in xs | where(predicate))

@queryize
def default_if_empty(xs, default_value):
  '''
  >>> [1, 2, 3] | default_if_empty('x') | to_tuple()
  (1, 2, 3)
  >>> [] | default_if_empty('x') | to_tuple()
  ('x',)
  '''
  has_value = False
  for x in xs:
    has_value = True
    yield x
  if not has_value:
    yield default_value

@queryize
def distinct(xs):
  '''
  >>> [1, 2, 3] | distinct() | to_tuple()
  (1, 2, 3)
  >>> [1, 2, 3, 1, 2, 3] | distinct() | to_tuple()
  (1, 2, 3)
  '''
  yielded_table = {}
  for x in xs:
    if yielded_table.get(x, False):
      pass
    else:
      yielded_table[x] = True
      yield x

@queryize
def element_at(xs, index):
  '''
  >>> [0, 1, 2, 3] | element_at(2)
  2
  >>> [0, 1, 2, 3] | element_at(4)
  Traceback (most recent call last):
    ...
  IndexError: Index 4 is out of range
  >>> [] | element_at(0)
  Traceback (most recent call last):
    ...
  IndexError: Index 0 is out of range
  '''
  sentinel = []
  result = xs | element_at_or_default(index, sentinel)
  if result is sentinel:
    raise IndexError('Index %d is out of range' % index)
  else:
    return result

@queryize
def element_at_or_default(xs, index, default_value):
  '''
  >>> [0, 1, 2, 3] | element_at_or_default(2, 'hi')
  2
  >>> [0, 1, 2, 3] | element_at_or_default(4, 'hi')
  'hi'
  >>> [] | element_at_or_default(0, 'hi')
  'hi'
  '''
  i = 0
  for x in xs:
    if i == index:
      return x
    else:
      i += 1
  return default_value

def empty():
  '''
  >>> empty() | any()
  False
  >>> empty() | count()
  0
  '''
  return ()

@queryize
def except_from(xs, xsd):
  '''
  >>> [0, 1, 2, 3] | except_from((1, 2)) | to_tuple()
  (0, 3)
  >>> # TODO: Is this behavior same as .NET Framework?
  >>> [0, 1, 0, 1, 2, 3] | except_from((1, 2, 1)) | to_tuple()
  (0, 0, 3)
  >>> [0, 1, 2, 3] | except_from(()) | to_tuple()
  (0, 1, 2, 3)
  >>> [0, 1, 2, 3] | except_from((4, 5, 6)) | to_tuple()
  (0, 1, 2, 3)
  '''
  for x in xs:
    if xsd | contains(x):
      pass
    else:
      yield x

@queryize
def first(xs, predicate = lambda x: True):
  '''
  >>> [0, 1, 2, 3] | first()
  0
  >>> [0, 1, 2, 3] | first(lambda x: 0 < x and x % 2 == 0)
  2
  >>> [] | first()
  Traceback (most recent call last):
    ...
  ValueError: Sequence must contain some element
  '''
  sentinel = []
  result = xs | first_or_default(sentinel, predicate)
  if result is sentinel:
    raise ValueError('Sequence must contain some element')
  else:
    return result

@queryize
def first_or_default(xs, default_value, predicate = lambda x: True):
  '''
  >>> [0, 1, 2, 3] | first_or_default('hi')
  0
  >>> [] | first_or_default('hi')
  'hi'
  >>> [0, 1, 2, 3] | first_or_default('hi', lambda x: 0 < x and x % 2 == 0)
  2
  >>> [1, 3, 5, 7] | first_or_default('hi', lambda x: 0 < x and x % 2 == 0)
  'hi'
  >>> [] | first_or_default('hi', lambda x: 0 < x and x % 2 == 0)
  'hi'
  '''
  for x in xs:
    if predicate(x):
      return x
  return default_value

@queryize
def last(xs, predicate = lambda x: True):
  '''
  >>> [0, 1, 2, 3] | last()
  3
  >>> [0, 1, 2, 3] | last(lambda x: x % 2 == 0)
  2
  >>> [] | last()
  Traceback (most recent call last):
    ...
  ValueError: Sequence must contain some element
  '''
  sentinel = []
  result = xs | last_or_default(sentinel, predicate)
  if result is sentinel:
    raise ValueError('Sequence must contain some element')
  else:
    return result

@queryize
def last_or_default(xs, default_value, predicate = lambda x: True):
  '''
  >>> [0, 1, 2, 3] | last_or_default('hi')
  3
  >>> [] | last_or_default('hi')
  'hi'
  >>> [0, 1, 2, 3] | last_or_default('hi', lambda x: x % 2 == 0)
  2
  >>> [1, 3, 5, 7] | last_or_default('hi', lambda x: x % 2 == 0)
  'hi'
  >>> [] | last_or_default('hi', lambda x: x % 2 == 0)
  'hi'
  '''
  sentinel = []
  last_value = sentinel
  for x in xs:
    if predicate(x):
      last_value = x
  if last_value is sentinel:
    return default_value
  else:
    return last_value

@queryize
def max(xs, y_from_x = lambda x: x):
  '''
  >>> range(10) | max()
  9
  >>> range(10) | max(lambda x: -x)
  0
  '''
  return __builtins__.max(y_from_x(x) for x in xs)

@queryize
def min(xs, y_from_x = lambda x: x):
  '''
  >>> range(10) | min()
  0
  >>> range(10) | min(lambda x: -x)
  -9
  '''
  return __builtins__.min(y_from_x(x) for x in xs)

@queryize
def order_by(xs, key_from_x):
  '''
  >>> range(10) | order_by(lambda x: -x) | to_tuple()
  (9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
  '''
  return OrderedSequence(xs, key_from_x)

@queryize
def reverse(xs):
  '''
  >>> range(10) | reverse() | to_tuple()
  (9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
  >>> [1] | reverse() | to_tuple()
  (1,)
  >>> [] | reverse() | to_tuple()
  ()
  '''
  return reversed(xs | to_list())

def repeat(x, n = None):
  '''
  >>> repeat(1, 3) | to_tuple()
  (1, 1, 1)
  >>> repeat(1) | take(5) | to_tuple()
  (1, 1, 1, 1, 1)
  '''
  if n is None:
    while True:
      yield x
  else:
    for i in range(n):
      yield x

@queryize
def select(xs, y_from_x):
  '''
  >>> range(10) | select(lambda x: x * x) | to_tuple()
  (0, 1, 4, 9, 16, 25, 36, 49, 64, 81)
  '''
  for x in xs:
    yield y_from_x(x)

@queryize
def select_with_index(xs, y_from_x_i):
  '''
  >>> [3, 2, 1] | select_with_index(lambda x, i: x * i) | to_tuple()
  (0, 2, 2)
  '''
  i = 0
  for x in xs:
    yield y_from_x_i(x, i)
    i += 1

@queryize
def select_many(xs, ys_from_x):
  '''
  >>> [1, 2, 3] | select_many(lambda x: [x] * x) | to_tuple()
  (1, 2, 2, 3, 3, 3)
  '''
  for x in xs:
    for y in ys_from_x(x):
      yield y

@queryize
def select_many_with_index(xs, ys_from_x_i):
  '''
  >>> [1, 2, 3] | select_many_with_index(lambda x, i: [x] * i) | to_tuple()
  (2, 3, 3)
  '''
  i = 0
  for x in xs:
    for y in ys_from_x_i(x, i):
      yield y
    i += 1

@queryize
def single(xs, predicate = lambda x: True):
  '''
  >>> [9] | single()
  9
  >>> [] | single()
  Traceback (most recent call last):
    ...
  ValueError: Sequence must contain only one element
  >>> range(10) | single()
  Traceback (most recent call last):
    ...
  ValueError: Sequence must contain only one element
  >>> [1, 9, 1] | single(lambda x: 1 < x)
  9
  >>> [] | single()
  Traceback (most recent call last):
    ...
  ValueError: Sequence must contain only one element
  >>> range(10) | single()
  Traceback (most recent call last):
    ...
  ValueError: Sequence must contain only one element
  '''
  sentinel = []
  result = xs | single_or_default(sentinel, predicate)
  if result is sentinel:
    raise ValueError('Sequence must contain only one element')
  else:
    return result

@queryize
def single_or_default(xs, default_value, predicate = lambda x: True):
  '''
  >>> [9] | single_or_default('hi')
  9
  >>> [] | single_or_default('hi')
  'hi'
  >>> range(10) | single_or_default('hi')
  Traceback (most recent call last):
    ...
  ValueError: Sequence must contain only one element
  >>> [1, 9, 1] | single_or_default('hi', lambda x: 1 < x)
  9
  >>> [] | single_or_default('hi')
  'hi'
  >>> range(10) | single_or_default('hi')
  Traceback (most recent call last):
    ...
  ValueError: Sequence must contain only one element
  '''
  sentinel = []
  the_value = sentinel
  for x in xs | where(predicate):
    if the_value is sentinel:
      the_value = x
    else:
      raise ValueError('Sequence must contain only one element')
  if the_value is sentinel:
    return default_value
  else:
    return the_value

@queryize
def skip(xs, n):
  '''
  >>> range(10) | skip(5) | to_tuple()
  (5, 6, 7, 8, 9)
  '''
  i = 0
  for x in xs:
    if n <= i:
      yield x
    i += 1

@queryize
def skip_while(xs, predicate):
  '''
  >>> [1, 3, 5, 7, 5, 3, 1] | skip_while(lambda x: x < 5) | to_tuple()
  (5, 7, 5, 3, 1)
  '''
  skipping = True
  for x in xs:
    if skipping and predicate(x):
      pass
    else:
      skipping = False
      yield x

@queryize
def skip_while_with_index(xs, predicate_with_index):
  '''
  >>> [1, 3, 5, 7] | skip_while_with_index(lambda x, i: x + i < 5) | to_tuple()
  (5, 7)
  '''
  skipping = True
  i = 0
  for x in xs:
    if skipping and predicate_with_index(x, i):
      pass
    else:
      skipping = False
      yield x
    i += 1

@queryize
def sum(xs, y_from_x = lambda x: x):
  '''
  >>> range(10) | sum()
  45
  >>> range(10) | sum(lambda x: -x)
  -45
  '''
  return __builtins__.sum(y_from_x(x) for x in xs)

@queryize
def take(xs, n):
  '''
  >>> range(10) | take(5) | to_tuple()
  (0, 1, 2, 3, 4)
  '''
  i = 0
  for x in xs:
    if i < n:
      yield x
    else:
      return
    i += 1

@queryize
def take_while(xs, predicate):
  '''
  >>> [1, 3, 5, 7, 5, 3, 1] | take_while(lambda x: x < 5) | to_tuple()
  (1, 3)
  '''
  for x in xs:
    if predicate(x):
      yield x
    else:
      break

@queryize
def take_while_with_index(xs, predicate_with_index):
  '''
  >>> [1, 3, 5, 7] | take_while_with_index(lambda x, i: x + i < 5) | to_tuple()
  (1, 3)
  '''
  i = 0
  for x in xs:
    if predicate_with_index(x, i):
      yield x
    else:
      break
    i += 1

@queryize
def then_by(ordered_xs, key_from_x):
  '''
  >>> (range(10)
  ...  | order_by(lambda x: x % 2)
  ...  | to_tuple())
  (0, 2, 4, 6, 8, 1, 3, 5, 7, 9)
  >>> (range(10)
  ...  | order_by(lambda x: x % 2)
  ...  | order_by(lambda x: -x)
  ...  | to_tuple())
  (9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
  >>> (range(10)
  ...  | order_by(lambda x: x % 2)
  ...  | then_by(lambda x: -x)
  ...  | to_tuple())
  (8, 6, 4, 2, 0, 9, 7, 5, 3, 1)
  >>> (range(10)                         # (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
  ...  | order_by(lambda x: x % 2)       # (0, 2, 4, 6, 8, 1, 3, 5, 7, 9)
  ...                                    #  -------------  -------------
  ...  | then_by(lambda x: x % 4 != 0)   # (0, 4, 8, 2, 6, 1, 3, 5, 7, 9)
  ...                                    #  -------  ----  -------------
  ...  | then_by(lambda x: -x)
  ...  | to_tuple())
  (8, 4, 0, 6, 2, 9, 7, 5, 3, 1)
  '''
  if isinstance(ordered_xs, OrderedSequence):
    return OrderedSequence(ordered_xs.xs,
                           lambda x: (ordered_xs.key_from_x(x), key_from_x(x)))
  else:
    raise ValueError('Sequence must be sorted with order_by')

@queryize
def to_dict(xs, key_from_x, value_from_x = lambda x: x):
  '''
  >>> d = ['apple', 'banana', 'cherry'] | to_dict(lambda x: x[0])
  >>> d['a']
  'apple'
  >>> d['b']
  'banana'
  >>> d['c']
  'cherry'
  >>> len(d.keys())
  3
  >>> d = ['ada', 'basic', 'cl'] | to_dict(lambda x: x[0], lambda x: len(x))
  >>> d['a']
  3
  >>> d['b']
  5
  >>> d['c']
  2
  >>> len(d.keys())
  3
  >>> d = ['ada', 'basic', 'csp'] | to_dict(lambda x: len(x), lambda x: x)
  Traceback (most recent call last):
    ...
  LookupError: Key 3 (for 'csp') is duplicate
  '''
  d = {}
  for x in xs:
    k = key_from_x(x)
    v = value_from_x(x)
    if k in d:
      raise LookupError('Key %r (for %r) is duplicate' % (k, x))
    else:
      d[k] = v
  return d

@queryize
def to_list(xs):
  '''
  >>> range(10) | to_list()
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
  '''
  return list(xs)

@queryize
def to_lookup(xs, key_from_x, value_from_x = lambda x: x):
  '''
  >>> d = ['ada', 'awk', 'bash', 'bcpl', 'c'] | to_lookup(lambda x: x[0])
  >>> d['a'] | to_tuple()
  ('ada', 'awk')
  >>> d['b'] | to_tuple()
  ('bash', 'bcpl')
  >>> d['c'] | to_tuple()
  ('c',)
  >>> len(d.keys())
  3
  >>> d = ['ada', 'awk', 'bash', 'bcpl', 'c'] | to_lookup(lambda x: x[0], len)
  >>> d['a'] | to_tuple()
  (3, 3)
  >>> d['b'] | to_tuple()
  (4, 4)
  >>> d['c'] | to_tuple()
  (1,)
  >>> len(d.keys())
  3
  '''
  d = {}
  for x in xs:
    k = key_from_x(x)
    v = value_from_x(x)
    if k in d:
      d[k].append(v)
    else:
      d[k] = [v]
  return d

@queryize
def to_tuple(xs):
  '''
  >>> range(10) | to_tuple()
  (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
  '''
  return tuple(xs)

@queryize
def where(xs, predicate):
  '''
  >>> range(10) | where(lambda x: x % 2 == 0) | to_tuple()
  (0, 2, 4, 6, 8)
  '''
  for x in xs:
    if predicate(x):
      yield x

@queryize
def where_with_index(xs, predicate_with_index):
  '''
  >>> [1, 3, 5, 7] | where_with_index(lambda x, i: i % 2 == 0) | to_tuple()
  (1, 5)
  '''
  i = 0
  for x in xs:
    if predicate_with_index(x, i):
      yield x
    i += 1




if __name__ == '__main__':
  import doctest
  doctest.testmod()

# __END__
