# -*- coding: utf-8 -*-


class ModelNotFound(RuntimeError):

    def __init__(self, model):
        self._model = model

        self.message = 'No query results found for model [%s]' % self._model.__name__

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
    default_detail = 'Invalid input.'

    def __init__(self, detail=None):
        if detail is None:
            self.detail = self.default_detail if detail is None else detail

    def __str__(self):
        return self.detail
