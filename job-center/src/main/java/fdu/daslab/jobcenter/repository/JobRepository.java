package fdu.daslab.jobcenter.repository;

import fdu.daslab.thrift.base.Job;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
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

    private List<Job> jobList = new ArrayList<>();

    public void saveJob(Job job) {
        jobList.add(job);
    }

    public Job findJob(String jobName) {
        return jobList.stream()
                .filter(job -> job.jobName.equals(jobName))
                .collect(Collectors.toList()).get(0);
    }

    public void updateJob(Job job) {
        jobList = jobList.stream()
                .map(oldJob -> {
                    if (job.jobName.equals(oldJob.jobName)) {
                        return job;
                    } else {
                        return oldJob;
                    }
                })
                .collect(Collectors.toList());
    }
}
