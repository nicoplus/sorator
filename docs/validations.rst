.. _Validations:

Validations
#############

Validations are used to ensure that only valid data is saved into your database.
``orator.orm.validators`` provides some pre-defined validation helpers. You can
use directly inside your model class definitions. For example

.. code-block:: python

  from orator.orm import Model
  class User(Model):

      @validates(presence=True)
      def validate_name(self, value):
          # more validate logic

``validates`` is a Python decorator. And it's parameters need to be validator name.
Here key/value are the validator name and corresponding validator class.

.. code-block:: python

  {
      'presence': PresenceValidator,
      'inclusion': InclusionValidator,
      'exclusion': ExclusionValidator,
      'pattern': PatternValidator,
      'length': LengthValidator,
      'numericality': NumericalityValidator,
      'uniqueness': UniquenessValidator,
      'range': RangeValidator,
  }


Besides, you can directly call the validator instance(``__call__`` method) to validate

.. code-block:: python

  from orator.orm import Model
  class User(Model):

      def validate_name(self, value):
          validator = PresenceValidator(True)
          validator(value) # will raise ValidationError if not valid


* NOTE: UniquenessValidator is special. Because it need the decorated function's meta data *


Validator Usage Listing
========================

PresenceValidator
----------------------

This validator validates that the specified value are not empty.

following case will be considered blank:

* ``None``
* ``''`` empty string
* ``'    '`` string only have whitespace

example:

.. code-block:: Python

  @validates(presence=True)
  def validate_name(self, value):
      pass

InclusionValidator
--------------------

This validator validates that the value are included in a given set.

example:

.. code-block:: python

  @validates(inclusion=('weiss', 'blake'))
  def validate_name(self, value):
      pass


The set should be a object that implements ``__contains__`` method, for example
``list``, ``tuple``, ``set`` .etc.

ExclusionValidator
--------------------

This validator validates that the value are not included in a
given set.

example:

.. code-block:: python

  @validates(exclusion=('weiss', 'blake'))
  def validate_name(self, value):
      pass


The set should be a object that implements ``__contains__`` method, for example
``list``, ``tuple``, ``set`` .etc.

PatternValidator
-----------------

This validator validates the value by testing whether they match a
given regular expression

example:

.. code-block:: python

  @validates(pattern=r'^[1-9]\d*$')
  def validate_name(self, value):
      pass

LengthValidator
-----------------

This validator validates the length of the values.

example: check the length **greater than** 0 and **less than** 10:

.. code-block:: python

  @validates(length={'minimum':0, 'maximum':10})
  def validate_phone(self, value):
      pass

The parameter should be a dict, and the key should be following combination

* ``minimum``
* ``maximum``
* ``in``
* ``eq``

NumericalityValidator
-----------------------

This validator validates that your value is numeric.
By default, it will match an optional sign followed by an integral or floating point number.

example:

.. code-block:: python

  @validates(numericality=True)
  def validate_num(self, value):
      pass


This validator is special. It can accept two kind of arguments, ``bool`` or ``dict``.

``bool`` type arugument decide whether enable this validator

``dict`` type arugment can have following key, and value is ``bool`` type

* ``only_integer`` value must be integer, not float point number
* ``even`` value must be even
* ``odd`` value must be odd

for example ``{'only_integer': True, 'odd': True}``

UniquenessValidator
--------------------

This validator validates that the value is unique in the database

.. code-block:: python

  @validates(uniqueness=True)
  def validate_uid(self, value):
      pass


This validator will split the function name ``validate_uid`` and use the ``uid``
as database field, then execute ``where_exists('uid', '=', value)`` in database

RangeValidator
----------------

This validator validates that whether the value is in given interval.

.. code-block:: python

  @validates(range={'gt': 0, 'lt': 10})
  def validate_amount(self, value):
      pass


The parameter should be ``dict`` type, and the key should be following combination

* ``gt``
* ``ge``
* ``lt``
* ``le``
* ``eq``
