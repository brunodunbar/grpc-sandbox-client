package com.brunodunbar;

import com.brunodunbar.grpc.sandbox.ExtracaoGrpc;
import com.brunodunbar.grpc.sandbox.ExtracaoRequest;
import com.brunodunbar.grpc.sandbox.GRpcWrapper;
import com.brunodunbar.grpc.sandbox.Services;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class GreeterController {

    public static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    @RequestMapping(path = "/send", method = RequestMethod.GET)
    public String send() {

        final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 6565)
                .usePlaintext(true)
                .build();

        final ExtracaoGrpc.ExtracaoStub stub = ExtracaoGrpc.newStub(channel);
        StreamObserver<Services.ExtracaoRequest> requestObserver = stub.extrair(new StreamObserver<Services.ExtracaoResponse>() {
            @Override
            public void onNext(Services.ExtracaoResponse response) {
                LOGGER.info("Response: " + GRpcWrapper.unwrap(response));
            }

            @Override
            public void onError(Throwable throwable) {
                LOGGER.error("Falha na comunicação", throwable);
            }

            @Override
            public void onCompleted() {
                //ok
            }
        });

        List<ExtracaoRequest> random = createRandom();
        random.stream()
                .map(GRpcWrapper::wrap)
                .forEach(requestObserver::onNext);

        return "ok";
    }

    private List<ExtracaoRequest> createRandom() {

        List<ExtracaoRequest> requests = new ArrayList<>();
        Random random = new Random();
        int max = random.nextInt(20);
        for (int i = 0; i < max; i++) {
            requests.add(new ExtracaoRequest.Builder().id((long) i).addValues(createRandomValues()).build());
        }
        return requests;
    }

    private List<Object> createRandomValues() {
        List<Object> objects = new ArrayList<>();
        Random random = new Random();
        int max = random.nextInt(20);
        for (int i = 0; i < max; i++) {
            objects.add(createRandomValue());
        }
        return objects;
    }

    private Object createRandomValue() {

        Random random = new Random();
        int value = random.nextInt(Services.ValueType.DOUBLE_VALUE);
        switch (Services.ValueType.forNumber(value)) {
            case NULL:
                return null;
            case STRING:
                return "TESTE STRIGN " + random.nextLong();
            case INTEGER:
                return random.nextInt();
            case LONG:
                return random.nextLong();
            case FLOAT:
                return random.nextFloat();
            case DOUBLE:
                return random.nextDouble();
        }

        return null;
    }


}
