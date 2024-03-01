from enum import StrEnum


class JobStatus(StrEnum):
    QUEUED = "queued"
    INITIATED = "initiated"
    VALIDATING = "validating"
    DECRYPTING = "decrypting"
    TRANSFORMING = "transforming"
    PSEUDONYMIZING = "pseudonymizing"
    ENRICHING = "enriching"  # legacy
    CONVERTING = "converting"  # legacy
    PARTITIONING = "partitioning"
    BUILT = "built"
    IMPORTING = "importing"
    COMPLETED = "completed"
    FAILED = "failed"


class Operation(StrEnum):
    BUMP = "BUMP"
    ADD = "ADD"
    CHANGE = "CHANGE"
    PATCH_METADATA = "PATCH_METADATA"
    SET_STATUS = "SET_STATUS"
    DELETE_DRAFT = "DELETE_DRAFT"
    REMOVE = "REMOVE"
    DELETE_ARCHIVE = "DELETE_ARCHIVE"


class ReleaseStatus(StrEnum):
    DRAFT = "DRAFT"
    PENDING_RELEASE = "PENDING_RELEASE"
    PENDING_DELETE = "PENDING_DELETE"
