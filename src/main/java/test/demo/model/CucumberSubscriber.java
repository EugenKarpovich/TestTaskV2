package test.demo.model;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;

public class CucumberSubscriber extends BaseSubscriber<Cucumber> {
    private final Integer maxVolume;
    private final Sinks.Many<Jar> jarSink;

    private final Object lock = new Object();
    private final List<Cucumber> cucumbers = new ArrayList<>();
    private int currentVolume = 0;

    public CucumberSubscriber(Integer maxVolume, Sinks.Many<Jar> jarSink) {
        this.maxVolume = maxVolume;
        this.jarSink = jarSink;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
        request(1);
    }

    @Override
    protected void hookOnNext(Cucumber cucumber) {
        int remainingVolume = cucumber.volumeInCmCube();

        while (remainingVolume > 0) {
            synchronized (lock) {
                int availableSpace = maxVolume - currentVolume;

                if (availableSpace <= 0) {
                    sendJarAndReset();
                    continue;
                }

                int volumeToAdd = Math.min(remainingVolume, availableSpace);
                cucumbers.add(new Cucumber(volumeToAdd));
                currentVolume += volumeToAdd;
                remainingVolume -= volumeToAdd;

                if (currentVolume == maxVolume) {
                    sendJarAndReset();
                }
            }
        }

        request(1);
    }

    @Override
    protected void hookOnComplete() {
        synchronized (lock) {
            if (!cucumbers.isEmpty()) {
                sendJarAndReset();
            }
        }
    }

    private void sendJarAndReset() {
        jarSink.tryEmitNext(new Jar(maxVolume, new ArrayList<>(cucumbers)));
        cucumbers.clear();
        currentVolume = 0;
    }
}
