package lodVader.enumerators;

/**
 * Possible states for the distribution STATUS field.
 * @author Ciro Baron Neto
 *
 */
public enum DistributionStatus {
	
	STREAMING, 

	STREAMED,

	SEPARATING_SUBJECTS_AND_OBJECTS,

	WAITING_TO_STREAM,

	CREATING_BLOOM_FILTER,

	CREATING_LINKSETS,

	ERROR,

	DONE,

	CREATING_JACCARD_SIMILARITY,

	UPDATING_LINK_STRENGTH
}
