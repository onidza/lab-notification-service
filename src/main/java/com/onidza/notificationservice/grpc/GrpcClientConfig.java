package com.onidza.notificationservice.grpc;

import com.onidza.grpc.client.ClientQueryServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.GrpcChannelFactory;

@Configuration
public class GrpcClientConfig {

    @Bean
    public ClientQueryServiceGrpc.ClientQueryServiceBlockingStub clientQueryServiceBlockingStub(
            GrpcChannelFactory channels
    ) {
        return ClientQueryServiceGrpc.newBlockingStub(
                channels.createChannel("backend")
        );
    }
}
