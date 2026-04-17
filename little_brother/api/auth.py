import functools

from flask import request, jsonify, current_app


def require_api_key(f):
    """Flask decorator that checks for a valid API key."""

    @functools.wraps(f)
    def decorated(*args, **kwargs):
        api_key = current_app.config.get("LB_API_KEY")
        if not api_key:
            return f(*args, **kwargs)

        provided = request.headers.get("X-API-Key") or request.args.get("api_key")
        if provided != api_key:
            return jsonify({"error": "Invalid or missing API key"}), 401
        return f(*args, **kwargs)

    return decorated
