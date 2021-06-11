package fdu.daslab.jobcenter.repository;

import fdu.daslab.thrift.base.Job;
import fdu.daslab.thrift.base.Stage;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *  对Job信息进行CRUD，目前先使用内存存储，为了开发方便
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 4:13 PM
 */
@Repository
public class JobRepository {

    private Map<String, Job> jobs = new HashMap<>();

    public void saveJob(Job job) {
        jobs.put(job.jobName, job);
    }

    public Job findJob(String jobName) {
        return jobs.get(jobName);
    }

    public void updateJob(Job job) {
        saveJob(job);
    }

    public void updateStage(Stage stage) {
        Job job = findJob(stage.jobName);
        job.subplans.put(stage.getStageId(), stage);
        saveJob(job);
    }
}
