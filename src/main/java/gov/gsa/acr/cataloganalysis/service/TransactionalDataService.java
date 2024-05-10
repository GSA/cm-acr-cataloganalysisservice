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
    public Flux<Void> replace(int numPartiions){return xsbDataRepository.deleteAll().thenMany(Flux.defer(() -> update(numPartiions)));}
    /**
     * It's very important that we move the records from staging to the final table inside of a DB transaction. In case
     * anything goes wrong, the transaction will be rolled back and the integrity of the production table will remain
     * intact. However, if we move ALL the records (70-80 Million) of them in one DB call, the query will take a long
     * time and a there is a chance of client timing out. The TCP channel might time out after being inactive for such
     * a long time, or the statement_timeout might kick in. To prevent all that this method moves data from staging to
     * production table partition by partition. Each partition has a reasonable number of rows, and that does not take
     * very long. But please realize that data movement from all partitions share the same transaction so even if there
     * is an error from one of the partition, the entire transaction will be rolled back and this code will ensure the
     * integrity of the production table.
     * @param numPartitions number of table partitions.
     * @return a void flux
     */
    public Flux<Void> update(int numPartitions){
        return Flux.range(0, numPartitions)
                .flatMap(partition -> {
                    try {
                        java.lang.reflect.Method method = xsbDataRepository.getClass().getMethod("moveXsbData_"+partition);
                        Mono<Void> voidMono = (Mono<Void>) method.invoke(xsbDataRepository);
                        return voidMono
                                .doFirst(() -> log.info("Moving xsb_data_temp_{}", partition))
                                .doOnSuccess(s-> log.info("Moved xsb_data_temp_{}", partition));
                    } catch (SecurityException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) { return Mono.error(new IllegalArgumentException(e)); }
                }, 2);

    }
}
