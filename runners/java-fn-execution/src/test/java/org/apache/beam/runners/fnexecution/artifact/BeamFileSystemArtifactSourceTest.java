package org.apache.beam.runners.fnexecution.artifact;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Tests for BeamFileSystemArtifactSource.
 */
@RunWith(JUnit4.class) public class BeamFileSystemArtifactSourceTest {

  BeamFileSystemArtifactStagingService stagingService = new BeamFileSystemArtifactStagingService();

  @Rule public TemporaryFolder stagingDir = new TemporaryFolder();

  @Test public void testStagingService() throws Exception {
    String stagingSession = "stagingSession";
    String stagingSessionToken = BeamFileSystemArtifactStagingService
        .generateStagingSessionToken(stagingSession, stagingDir.newFolder().getPath());
    List<ArtifactApi.ArtifactMetadata> metadata = new ArrayList<>();

    metadata.add(ArtifactApi.ArtifactMetadata.newBuilder().setName("file1").build());
    putArtifactContents(stagingSessionToken, "first", "file1");

    metadata.add(ArtifactApi.ArtifactMetadata.newBuilder().setName("file2").build());
    putArtifactContents(stagingSessionToken, "second", "file2");

    String stagingToken = commitManifest(stagingSessionToken, metadata);

    BeamFileSystemArtifactSource artifactSource = new BeamFileSystemArtifactSource(stagingToken);
    Assert.assertEquals("first", getArtifactContents(artifactSource, "file1"));
    Assert.assertEquals("second", getArtifactContents(artifactSource, "file2"));
    Assert.assertThat(artifactSource.getManifest().getArtifactList(),
        containsInAnyOrder(metadata.toArray(new ArtifactApi.ArtifactMetadata[0])));
  }

  private String commitManifest(String stagingSessionToken,
      List<ArtifactApi.ArtifactMetadata> artifacts) {
    String[] stagingTokenHolder = new String[1];
    stagingService.commitManifest(
        ArtifactApi.CommitManifestRequest.newBuilder().setStagingSessionToken(stagingSessionToken)
            .setManifest(ArtifactApi.Manifest.newBuilder().addAllArtifact(artifacts)).build(),
        new StreamObserver<ArtifactApi.CommitManifestResponse>() {

          @Override public void onNext(ArtifactApi.CommitManifestResponse commitManifestResponse) {
            stagingTokenHolder[0] = commitManifestResponse.getRetrievalToken();
          }

          @Override public void onError(Throwable throwable) {
            throw new RuntimeException(throwable);
          }

          @Override public void onCompleted() {
          }
        });

    return stagingTokenHolder[0];
  }

  private void putArtifactContents(String stagingSessionToken, String contents, String name) {
    StreamObserver<ArtifactApi.PutArtifactRequest> outputStreamObserver = stagingService
        .putArtifact(new StreamObserver<ArtifactApi.PutArtifactResponse>() {

          @Override public void onNext(ArtifactApi.PutArtifactResponse putArtifactResponse) {
          }

          @Override public void onError(Throwable throwable) {
            throw new RuntimeException(throwable);
          }

          @Override public void onCompleted() {
          }
        });

    outputStreamObserver.onNext(ArtifactApi.PutArtifactRequest.newBuilder().setMetadata(
        ArtifactApi.PutArtifactMetadata.newBuilder()
            .setMetadata(ArtifactApi.ArtifactMetadata.newBuilder().setName(name).build())
            .setStagingSessionToken(stagingSessionToken)).build());
    outputStreamObserver.onNext(ArtifactApi.PutArtifactRequest.newBuilder().setData(
        ArtifactApi.ArtifactChunk.newBuilder()
            .setData(ByteString.copyFrom(contents, StandardCharsets.UTF_8))).build());
    outputStreamObserver.onCompleted();
  }

  private String getArtifactContents(ArtifactSource artifactSource, String name)
      throws IOException {
    StringBuilder contents = new StringBuilder();
    artifactSource.getArtifact(name, new StreamObserver<ArtifactApi.ArtifactChunk>() {

      @Override public void onNext(ArtifactApi.ArtifactChunk artifactChunk) {
        contents.append(artifactChunk.getData().toString(StandardCharsets.UTF_8));
      }

      @Override public void onError(Throwable throwable) {
        throw new RuntimeException(throwable);
      }

      @Override public void onCompleted() {
      }
    });
    return contents.toString();
  }
}
