package io.zeebe.gateway.impl.probes.liveness;

import java.io.File;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;

@ConfigurationProperties(prefix = "management.health.liveness.diskspace")
public class LivenessDiskSpaceHealthIndicatorProperties {

  private File path = new File(".");

  private DataSize threshold = DataSize.ofMegabytes(1);

  public File getPath() {
    return this.path;
  }

  public void setPath(File path) {
    Assert.isTrue(path.exists(), () -> "Path '" + path + "' does not exist");
    Assert.isTrue(path.canRead(), () -> "Path '" + path + "' cannot be read");
    this.path = path;
  }

  public DataSize getThreshold() {
    return this.threshold;
  }

  public void setThreshold(DataSize threshold) {
    Assert.isTrue(!threshold.isNegative(), "threshold must be greater than or equal to 0");
    this.threshold = threshold;
  }
}
