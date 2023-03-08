from enum import Enum


class JobStatus(str, Enum):
    QUEUED = 'queued'
    INITIATED = 'initiated'
    VALIDATING = 'validating'
    TRANSFORMING = 'transforming'
    PSEUDONYMIZING = 'pseudonymizing'
    ENRICHING = 'enriching'
    CONVERTING = 'converting'
    BUILT = 'built'
    IMPORTING = 'importing'
    COMPLETED = 'completed'
    FAILED = 'failed'


class Operation(str, Enum):
    BUMP = 'BUMP'
    ADD = 'ADD'
    CHANGE = 'CHANGE'
    PATCH_METADATA = 'PATCH_METADATA'
    SET_STATUS = 'SET_STATUS'
    DELETE_DRAFT = 'DELETE_DRAFT'
    REMOVE = 'REMOVE'
    MOVE_ARCHIVED_TO_INPUT = 'MOVE_ARCHIVED_TO_INPUT'
    DELETE_ARCHIVE = 'DELETE_ARCHIVE'


class ReleaseStatus(str, Enum):
    DRAFT = 'DRAFT'
    PENDING_RELEASE = 'PENDING_RELEASE'
    PENDING_DELETE = 'PENDING_DELETE'
