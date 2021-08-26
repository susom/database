package com.github.susom.database.example;

import java.util.Date;

/**
 * Simple bean for use with SampleDao.
 */
public class Sample {
  private Long sampleId;
  private String name;
  private Integer updateSequence;
  private Date updateTime;

  public Long getSampleId() {
    return sampleId;
  }

  public void setSampleId(Long sampleId) {
    this.sampleId = sampleId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getUpdateSequence() {
    return updateSequence;
  }

  public void setUpdateSequence(Integer updateSequence) {
    this.updateSequence = updateSequence;
  }

  public Date getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(Date updateTime) {
    this.updateTime = updateTime;
  }
}
