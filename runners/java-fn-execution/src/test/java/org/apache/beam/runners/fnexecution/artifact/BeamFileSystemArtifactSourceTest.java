package org.apache.beam.runners.fnexecution.artifact;

import io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Tests for BeamFileSystemArtifactSource.
 */
@RunWith(JUnit4.class)
public class BeamFileSystemArtifactSourceTest {
  @Test
  public void testPlaceholder() {}
}
