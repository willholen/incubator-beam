package org.apache.beam.runners.fnexecution.artifact;

import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;

/**
 * An ArtifactSource suitable for retrieving artifacts uploaded via
 * {@link BeamFileSystemArtifactStagingService}.
 */
public class BeamFileSystemArtifactSource implements ArtifactSource {

  private final String retrievalToken;
  private final DfsArtifactRetrievalService retrievalService;

  public BeamFileSystemArtifactSource(String retrievalToken) {
    this.retrievalToken = retrievalToken;
    this.retrievalService = new DfsArtifactRetrievalService();  // TODO: Make that use this?
  }

  @Override public ArtifactApi.Manifest getManifest() throws IOException {
    try {
      return DfsArtifactRetrievalService.loadManifestFrom(retrievalToken).getManifest();
    } catch (Exception exn) {
      throw new IOException(exn);
    }
  }

  @Override public void getArtifact(String name,
      StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) {
    retrievalService.getArtifact(
        ArtifactApi.GetArtifactRequest.newBuilder().setName(name).setRetrievalToken(retrievalToken)
            .build(), responseObserver);
  }
}
