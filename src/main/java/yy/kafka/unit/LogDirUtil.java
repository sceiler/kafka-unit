package yy.kafka.unit;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class LogDirUtil
{
  private static final Logger LOGGER = LogManager.getLogger(LogDirUtil.class);

  /**
   * Creates a temp directory with the specified prefix and attaches a
   * shutdown hook to delete the directory on jvm exit.
   *
   * @param prefix temporary log dir prefix
   * @return file object
   */
  public static File prepareLogDir(String prefix)
  {
    final File logDir;
    try
    {
      logDir = Files.createTempDirectory(prefix).toFile();
    }
    catch (IOException e)
    {
      throw new RuntimeException(
              "Unable to create temp folder with prefix " + prefix, e);
    }
    logDir.deleteOnExit();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try
      {
        FileUtils.deleteDirectory(logDir);
      }
      catch (IOException e)
      {
        LOGGER.warn("Problems deleting temporary directory "
                + logDir.getAbsolutePath(), e);
      }
    }));
    return logDir;
  }
}