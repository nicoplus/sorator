# -*- coding: utf-8 -*-
import json


class ModelNotFound(RuntimeError):

    def __init__(self, model):
        self._model = model

        self.message = ('No query results found for model [%s]' %
                        self._model.__name__)

    def __str__(self):
        return self.message


class MassAssignmentError(RuntimeError):
    pass


class RelatedClassNotFound(RuntimeError):

    def __init__(self, related):
        self._related = related

        self.message = 'The related class for "%s" does not exists' % related

    def __str__(self):
        return self.message


class ValidationError(ValueError):
    detail = 'Invalid input.'

    def __init__(self, detail=None):
        if detail is None:
            return
        try:
            self.detail = json.dumps(detail)
        except TypeError:
            self.detail = str(detail)

    def __str__(self):
        return self.detail
