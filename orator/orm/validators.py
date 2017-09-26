import re
import functools
from ..exceptions.orm import ValidationError


class PresenceValidator:

    def __init__(self, status):
        self._status = status if isinstance(status, bool) else False

    def __call__(self, value):
        if not self._status:
            return value

        if isinstance(value, bytes):
            if value.strip(r' ') != '':
                return value

        if isinstance(value, str):
            if value.strip(' ') != '':
                return value

        if value is not None:
            return value

        return ValidationError("")


class InclusionValidator:

    def __init__(self, candidate):
        self._candidate = candidate

    def __call__(self, value):
        if value not in self._candidate:
            raise ValidationError("")
        return value


class ExclusionValidator:

    def __init__(self, candidate):
        self._candidate = candidate

    def __call__(self, value):
        if value in self._candidate:
            raise ValidationError("")
        return value


class PatternValidator:

    def __init__(self, pattern):
        self._pattern = re.compile(pattern)

    def __call__(self, value):
        if not self._pattern.match(value):
            raise ValidationError("")
        return value


class NumericalityValidator:

    NUM_REGEX = re.compile(r'[+-]?\d+')

    def __init__(self, args):
        self._status = False
        if isinstance(args, bool):
            self._status = args

        self._only_integer = args.get('only_integer')
        self._greater_than = args.get('greater_than', float('-INF'))
        self._greater_than_or_equal_to = args.get(
            'greater_than_or_equal_to ', float('-INF'))
        self._equal_to = args.get('equal_to ')
        self._less_than = args.get('less_than', float('INF'))
        self._less_than_or_equal_to = args.get(
            'less_than_or_equal_to ') or float('-INF')
        self._odd = args.get('odd')
        self._even = args.get('even')

    def __call__(self, value):
        copy = value
        if isinstance(value, bytes):
            value = value.decode()
        if isinstance(value, str):
            if self._only_integer:
                if not self.NUM_REGEX.match(value.strip()):
                    raise ValidationError("")
                value = int(value)
            else:
                value = float(value)
        assert isinstance(value, (int, float))

        if self._equal_to and self._equal_to != value:
            raise ValidationError("")

        if self._odd and value % 2 != 1:
            raise ValidationError("must be odd")

        if self._even and value % 2 != 0:
            raise ValidationError("must be even")

        if not self._greater_than_or_equal_to <= value:
            raise ValidationError(
                "value must be greater than or equal to {}".format(
                    self._greater_than_or_equal_to))
        if not self._less_than_or_equal_to >= value:
            raise ValidationError(
                "value must be less than or equal to {}".format(
                                      self._less_than_or_equal_to))

        if not self._greater_than < value:
            raise ValidationError(
                "value must be greater than {}".format(self._greater_than))
        if not self._less_than > value:
            raise ValidationError(
                "value must be less than {}".format(self._less_than))

        return copy


class LengthValidator:

    def __init__(self, kwargs):
        self._minimum = kwargs.get('minimum', 0)
        self._maximum = kwargs.get('maximum', float('INF'))
        _in = kwargs.get('in')
        if _in is not None:
            self._minimum, self._maximum = _in
        self._equal = kwargs.get('equal')

    def __call__(self, value):
        length = len(value)
        if self._equal and length != self._equal:
            raise ValidationError("length must be {}".format(self._equal))
        if not self._minimum < length:
            raise ValidationError(
                "length must be greater than {}".format(self._minimum))
        if not self._maximum > length:
            raise ValidationError(
                "length must be less than {}".format(self._maximum))
        return value


class ValidatorDispatcher:

    mapping = {
        'presence': PresenceValidator,
        'inclusion': InclusionValidator,
        'exclusion': ExclusionValidator,
        'pattern': PatternValidator,
        'length': LengthValidator,
        'numericality': NumericalityValidator,
    }

    @classmethod
    def dispathcer(cls, kwargs):
        chain = []
        for validator_name, args in kwargs.items():
            validator_cls = cls.mapping.get(validator_name)
            if validator_cls is not None:
                chain.append(validator_cls(args))
        return chain


class validates: # noqa

    def __init__(self, **kwargs):
        self._chain = ValidatorDispatcher.dispathcer(kwargs)

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(instance, *args):
            value = args.pop(0)
            for validate in self.chain:
                value = validate(value)
        return wrapper
