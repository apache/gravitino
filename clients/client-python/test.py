from gravitino.dto.responses.error_response import ErrorResponse

er = ErrorResponse.rest_error("XDXD")

er2 = ErrorResponse(1, "2", "XDXDXD", ["1", "2", "3"])

print(er._code)
print(er._message)
print(er)

print(er2)

print(ErrorResponse.__class__.__name__)
print(type(ErrorResponse).__name__)
print(ErrorResponse.__name__)