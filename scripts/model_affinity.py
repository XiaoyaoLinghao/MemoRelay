"""Request metadata for MemoRelay's existing model gateway route.

Does not alter model, credentials, payload, job state or retry policy.
"""
import hashlib


def with_model_affinity(request):
    existing = {name.lower() for name, _ in request.header_items()}
    if 'x-opencode-session' not in existing:
        # Canonical extraction payload includes source identity/revision and
        # processor prompt. Same request retries keep affinity; changed source
        # or processing input gets a different session. Raw source never enters
        # the header.
        digest = hashlib.sha256(request.data or b'').hexdigest()[:32]
        request.add_header('X-Opencode-Session', 'memorelay-extract-' + digest)
    if 'user-agent' not in existing:
        request.add_header('User-Agent', 'MemoRelay/2.0 (background memory extraction)')
    return request
