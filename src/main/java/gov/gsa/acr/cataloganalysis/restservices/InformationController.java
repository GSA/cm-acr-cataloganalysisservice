package gov.gsa.acr.cataloganalysis.restservices;

import io.swagger.v3.oas.annotations.Hidden;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Hidden
@RestController
public class InformationController extends BaseController{

    @GetMapping(value={"", "/", "welcome"})
    public String greet() {
        return "Welcome to Catalog Analysis";
    }

    @GetMapping("/info")
    public String info(){
        return "A service for analyzing catalogs in ACR";
    }
}
