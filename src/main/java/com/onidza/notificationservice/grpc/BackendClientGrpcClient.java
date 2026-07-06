package com.onidza.notificationservice.grpc;

import com.onidza.grpc.client.ClientForNotificationResponse;
import com.onidza.grpc.client.ClientQueryServiceGrpc;
import com.onidza.grpc.client.GetClientRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class BackendClientGrpcClient {

    private final ClientQueryServiceGrpc.ClientQueryServiceBlockingStub stub;

    public ClientForNotificationResponse getClientForNotificationResponse(Long clientId) {
        try {
            return stub
                    .withDeadlineAfter(500, TimeUnit.MILLISECONDS)
                    .getClientForNotification(
                            GetClientRequest.newBuilder()
                                    .setClientId(clientId)
                                    .build()
                    );

        } catch (StatusRuntimeException ex) {
            if (ex.getStatus().getCode() == Status.Code.NOT_FOUND) {
                log.warn("Client not found in backend, clientId = {}", clientId);
            } else {
                log.error("gRPC call to backend failed, clientId = {}, status = {}",
                        clientId, ex.getStatus(), ex);
            }

            throw ex;
        }
    }
}
