package test.demo.controller;


import reactor.core.publisher.Flux;
import test.demo.service.PickerService;
import test.demo.model.Cucumber;
import test.demo.model.Jar;

public class PickerController {

    private final PickerService pickerService;

    public PickerController(PickerService pickerService) {
        this.pickerService = pickerService;
    }

    public Flux<Jar> pickCucumbers(Flux<Cucumber> cucumbers) {
        return pickerService.distributionCucumbersIntoJars(cucumbers);
    }
}
