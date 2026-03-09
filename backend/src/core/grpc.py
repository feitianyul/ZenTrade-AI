
import grpc

from src.core.trace import get_trace_id


class TracingInterceptor(grpc.UnaryUnaryClientInterceptor):
    def intercept_unary_unary(self, continuation, client_call_details, request):
        metadata = []
        if client_call_details.metadata:
            metadata = list(client_call_details.metadata)
        
        metadata.append(("x-trace-id", get_trace_id()))
        
        new_details = client_call_details._replace(metadata=metadata)
        return continuation(new_details, request)

def create_secure_channel(target: str, cert_path: str = None) -> grpc.Channel:
    if cert_path:
        with open(cert_path, "rb") as f:
            creds = grpc.ssl_channel_credentials(f.read())
        channel = grpc.secure_channel(target, creds)
    else:
        channel = grpc.insecure_channel(target)
    
    return grpc.intercept_channel(channel, TracingInterceptor())
