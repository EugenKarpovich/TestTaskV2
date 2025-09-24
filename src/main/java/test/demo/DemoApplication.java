package test.demo;

import reactor.core.publisher.Flux;
import test.demo.service.PickerService;
import test.demo.controller.PickerController;
import test.demo.model.Cucumber;

import java.util.List;

public class DemoApplication {

	public static void main(String[] args) {
		List<Cucumber> cucumberList = List.of(
				new Cucumber(30),
				new Cucumber(4),
				new Cucumber(6),
				new Cucumber(2),
				new Cucumber(1),
				new Cucumber(4),
				new Cucumber(4),
				new Cucumber(2)
		);

		Flux<Cucumber> cucumbers = Flux.fromIterable(cucumberList);

		PickerService pickerService = new PickerService();
		PickerController pickerController = new PickerController(pickerService);

		pickerController.pickCucumbers(cucumbers)
				.subscribe(jar -> System.out.println("Итоговая банка: " + jar));

	}

}
