package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;


/**
 * In order for annotated transactions to work, the @Transactional annotation had to be applied at the Bean level.
 * Previously this annotation was applied to a method within the AnalysisDataProcessingService bean. But the AOP code was not
 * generated or inserted correctly.
 * According to the documentation found at
 * <a href="https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/annotations.html">...</a>
 * In proxy mode (which is the default), only external method calls coming in through the proxy are intercepted. This
 * means that self-invocation (in effect, a method within the target object calling another method of the target object)
 * does not lead to an actual transaction at runtime even if the invoked method is marked with @Transactional. Also, the
 * proxy must be fully initialized to provide the expected behavior, so you should not rely on this feature in your
 * initialization code (that is, @PostConstruct).
 * Also,
 * The @Transactional annotation is typically used on methods with public visibility. As of 6.0, protected or
 * package-visible methods can also be made transactional for class-based proxies by default. Note that transactional
 * methods in interface-based proxies must always be public and defined in the proxied interface. For both kinds of
 * proxies, only external method calls coming in through the proxy are intercepted.
 */
@Service
@Transactional(rollbackFor = {Exception.class, Error.class, RuntimeException.class})
@Slf4j
public class TransactionalDataService {
    private final XsbDataRepository xsbDataRepository;

    public TransactionalDataService(XsbDataRepository xsbDataRepository) {
        this.xsbDataRepository = xsbDataRepository;
    }

    public Mono<Void> update(){return xsbDataRepository.moveXsbData();}
    public Mono<Void> replace(){return xsbDataRepository.deleteAll().then(Mono.defer(this::update));}

    public Flux<Void> updatePartitionByPartition(int numPartitions){
        return Flux.range(0, numPartitions)
                .flatMap(partition -> {
                    try {
                        java.lang.reflect.Method method = xsbDataRepository.getClass().getMethod("moveXsbData_"+partition);
                        Mono<Void> voidMono = (Mono<Void>) method.invoke(xsbDataRepository);
                        return voidMono.doOnSuccess(s-> log.info("Finished moving data from partition xsb_data_temp_{} to xsb_data", partition));
                    } catch (SecurityException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) { return Mono.error(new IllegalArgumentException(e)); }
                }, 2);

    }

    // TBD this is just for testing. Delete once code is tested thoroughly
    public Mono<Void> testRollbackUpdate(){return update().then(Mono.error(new Exception("Update Forced Error")));}
    // TBD this is just for testing. Delete once code is tested thoroughly
    public Mono<Void> testRollbackReplace(){return replace().then(Mono.error(new Exception("Replace Forced Error")));}
}
