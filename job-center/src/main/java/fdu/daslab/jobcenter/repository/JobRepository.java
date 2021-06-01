package fdu.daslab.jobcenter.repository;

import fdu.daslab.thrift.base.Job;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 *  对Job信息进行CRUD，目前先使用内存存储，为了开发方便
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 4:13 PM
 */
@Repository
public class JobRepository {

    private List<Job> jobList = new ArrayList<>();

    public void saveJob(Job job) {
        jobList.add(job);
    }

    public Job findJob(String jobName) {
        return jobList.stream()
                .filter(job -> job.jobName.equals(jobName))
                .c
    }

    public void updateJob(Job job) {

    }
}
