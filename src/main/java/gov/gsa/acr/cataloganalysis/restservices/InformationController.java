package gov.gsa.acr.cataloganalysis.restservices;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InformationController extends BaseController{

    @GetMapping("/info")
    public String info(){
        return "A service for analyzing catalogs in ACR";
    }
}
