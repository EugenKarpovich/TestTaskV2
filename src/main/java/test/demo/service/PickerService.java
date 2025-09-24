package test.demo.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import test.demo.model.Cucumber;
import test.demo.model.CucumberSubscriber;
import test.demo.model.Jar;

public class PickerService {

    private static final int MAX_VOLUME = 10;

    public Flux<Jar> distributionCucumbersIntoJars(Flux<Cucumber> cucumbers) {
        Sinks.Many<Jar> jarSink = Sinks.many().unicast().onBackpressureBuffer();

        cucumbers
                .subscribe(new CucumberSubscriber(MAX_VOLUME, jarSink));

        return jarSink.asFlux();
    }

}
