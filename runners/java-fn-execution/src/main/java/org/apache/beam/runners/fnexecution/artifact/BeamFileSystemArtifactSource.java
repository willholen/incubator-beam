package org.apache.beam.runners.fnexecution.artifact;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;

import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;

/**
 * An ArtifactSource suitable for retrieving artifacts uploaded via
 * {@link BeamFileSystemArtifactStagingService}.
 */
public class BeamFileSystemArtifactSource implements ArtifactSource {

  private static final int CHUNK_SIZE = 2 << 20; // 10MB.

  private final String retrievalToken;
  private ArtifactApi.ProxyManifest proxyManifest;

  public BeamFileSystemArtifactSource(String retrievalToken) {
    this.retrievalToken = retrievalToken;
  }

  public static BeamFileSystemArtifactSource create(String artifactToken) {
    return new BeamFileSystemArtifactSource(artifactToken);
  }

  @Override
  public ArtifactApi.Manifest getManifest() throws IOException {
    return getProxyManifest().getManifest();
  }

  @Override
  public void getArtifact(String name,
      StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) throws IOException {
    ReadableByteChannel artifact = FileSystems
        .open(FileSystems.matchNewResource(lookupUri(name), false));
    ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE);
    while (artifact.read(buffer) > -1) {
      buffer.flip();
      responseObserver.onNext(
          ArtifactApi.ArtifactChunk.newBuilder().setData(ByteString.copyFrom(buffer)).build());
      buffer.clear();
    }
  }

  private String lookupUri(String name) throws IOException {
    for (ArtifactApi.ProxyManifest.Location location : getProxyManifest().getLocationList()) {
      if (location.getName().equals(name)) {
        return location.getUri();
      }
    }
    throw new IllegalArgumentException("No such artifact: " + name);
  }

  private ArtifactApi.ProxyManifest getProxyManifest() throws IOException {
    if (proxyManifest == null) {
      ArtifactApi.ProxyManifest.Builder builder = ArtifactApi.ProxyManifest.newBuilder();
      JsonFormat.parser().merge(Channels.newReader(
          FileSystems.open(FileSystems.matchNewResource(retrievalToken, false /* is directory */)),
          StandardCharsets.UTF_8.name()), builder);
      proxyManifest = builder.build();
    }
    return proxyManifest;
  }
}
