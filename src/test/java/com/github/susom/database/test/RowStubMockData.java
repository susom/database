package com.github.susom.database.test;

import java.time.LocalDate;
import java.util.Date;

/**
 * Simple bean for use by RowStubMockDao mock testing the RowStub implementation
 */
public class RowStubMockData {
  private Long dataId;
  private String name;
  private LocalDate localDate;
  private Integer updateSequence;
  private Date updateTime;

  public Long getDataId() {
    return dataId;
  }

  public void setDataId(Long dataId) {
    this.dataId = dataId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public LocalDate getLocalDate() {
    return localDate;
  }

  public void setLocalDate(LocalDate localDate) {
    this.localDate = localDate;
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
