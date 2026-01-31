package fr.orange.drs.refmob.tasklet;

import fr.orange.drs.refmob.exception.BatchProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
@Slf4j
public class DuplicateCellIdentifierFilterTasklet implements Tasklet {

    private final PlatformTransactionManager transactionManager;
    private final JdbcTemplate jdbcTemplate;
    private int ignored3GCount = 0;
    private int ignored4GCount = 0;
    //private int ignored5GCount = 0;

    public DuplicateCellIdentifierFilterTasklet(PlatformTransactionManager transactionManager, JdbcTemplate jdbcTemplate) {
        this.transactionManager = transactionManager;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

        String duplicateCellIdDeleteSql = """
                DELETE from temp_fg_cell where id in (
                select id from temp_fg_cell fc
                where fc.eci is not null
                and (select count(*) from temp_fg_cell fc2  where fc.eci = fc2.eci) > 1
                and is_radio = false
                UNION
                select id from temp_fg_cell fc
                where fc.nci is not null
                and (select count(*) from temp_fg_cell fc2  where fc.nci = fc2.nci) > 1
                and is_radio = false
                UNION
                select id from temp_fg_cell fc
                where fc.lcid is not null
                and (select count(*) from temp_fg_cell fc2  where fc.lcid = fc2.lcid) > 1
                and is_radio = false)
                """;
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.executeWithoutResult(status -> {
            try {
                jdbcTemplate.update(duplicateCellIdDeleteSql);
                deleteCellWithDuplicateEci();
                deleteCellWithDuplicateNci();
            } catch (Exception e) {
                log.error("Erreur lors de la suppression des identifiants cellule dupliqués", e);
                throw new BatchProcessingException("Erreur lors de la suppression des identifiants cellule dupliqués, rollback effectué",e);
            }
        });
        return RepeatStatus.FINISHED;
    }

    private void deleteCellWithDuplicateEci() {
        String duplicateEciSql = """
                DELETE from temp_fg_cell where eci in (
                select eci from temp_fg_cell
                group by eci
                having count(*) > 1)
                RETURNING eci
                """;
        List<Long> deletedCellEci = jdbcTemplate.queryForList(
                duplicateEciSql,
                Long.class
        );
        if (!deletedCellEci.isEmpty()) {
            Set<Long> uniqueDeletedCellEci = new HashSet<>(deletedCellEci);
            ignored4GCount = deletedCellEci.size();
            log.warn("{} Lignes ignorées avec un ECI doublon sur deux/des cellules différentes. Eci concernés : {}", ignored4GCount,uniqueDeletedCellEci);
        }
    }

    private void deleteCellWithDuplicateNci() {
        String duplicateNciSql = """
                DELETE from temp_fg_cell where nci in (
                select nci from temp_fg_cell
                group by nci
                having count(*) > 1)
                RETURNING nci
                """;
        List<Long> deletedCellNci = jdbcTemplate.queryForList(
                duplicateNciSql,
                Long.class
        );
        if (!deletedCellNci.isEmpty()) {
            Set<Long> uniqueDeletedCellNci = new HashSet<>(deletedCellNci);
            ignored5GCount = deletedCellNci.size();
            log.warn("{} Lignes ignorées avec un NCI doublon sur deux/des cellules différentes. Nci concernés : {}", ignored5GCount,uniqueDeletedCellNci);
        }
    }

    public int getIgnored3GCount() { return ignored3GCount; }
    public int getIgnored4GCount() { return ignored4GCount; }
    public int getIgnored5GCount() { return ignored5GCount; }
}

