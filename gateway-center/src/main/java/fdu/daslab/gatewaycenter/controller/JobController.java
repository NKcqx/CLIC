package fdu.daslab.gatewaycenter.controller;

import fdu.daslab.gatewaycenter.service.JobWebService;
import fdu.daslab.gatewaycenter.utils.R;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.*;

/**
 * @author zjchenn
 * @description
 * @since 2021/6/9 下午4:42
 */

@RestController
@RequestMapping("/job")
public class JobController {

    @Autowired
    private JobWebService jobWebService;

    @PostMapping("/submit")
    public R submitJob(@RequestParam String jobName, @RequestBody String plan){
        System.out.println(jobName+plan);
//        jobWebService.submit();
        return R.ok();
    }
}
