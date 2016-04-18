########################################################################
#
#	generateParametersBTJCompact.py
#
#	The goal of this script is to provide an easy way to generate parameters
#	for the Binary Theta Joins project.
#
#	The user has to provide parameters at the part 1 and at the parts 2.1
#	and 2.2 the systems generates the parameters.
#
#	Author: John Koumarelas
#
########################################################################
########################################################################
########################################################################
#!/usr/bin/python
import os
import shutil
from collections import defaultdict

########################################################################
########################################################################
#
#	Part 1
#
#	In this part, we specify the parameters to be generated. More specifically
#	we specify lists of parameters and a cross-product of those gets generated in the end.
#
########################################################################

def sortNormalizeWeights(weights):
	newWeights = []
	for weightsTok in weights:
		newWeightsTok = ""
		sumWeights = 0.0
		weightsCase = []
		
		weightToks = weightsTok.split("-")
		for weightTok in weightToks:
			weightsCase.append(weightTok)
			sumWeights += float(weightTok.split("_")[1])
		
		sortedWeightsCase = sorted(weightsCase)
		for i in range(0,len(sortedWeightsCase)):
			name = sortedWeightsCase[i].split('_')[0]
			weight = float(sortedWeightsCase[i].split('_')[1])
			newWeightsTok += name + "_" + str(weight/sumWeights) + "-"
		newWeightsTok = newWeightsTok[:-1]
		
		newWeights.append(newWeightsTok)
	return newWeights

# Creation of Partition Matrices (from real data). NOT virtual...
realPartitionMatrix = True#True

# Rearranging rows and columns.
rearrangements = True

# Partitioning
partitioning = True

# MBucketI - MapReduce execution
mbucketi = False

datasetsFolder = "datasets"
dataset = "solarAltitude_1m"

#executable path for TSPk. btj directory already exists to the same path with the root directory "datasets"
tspkPath = "btj/tspk"

#the folder which contains the generated .txt parameters file
parametersFolder = "parameters"

#Overrides the default hadoop parameter which is 10minutes
jobMaxExecutionHours = '30' # 30 hours

#the number of buckets used. We only use 100 buckets in our experiments
bucketsVls = [100]

#The width of the band depends on the skewness of the dataset. bucket[i+sparsity]-bucket[i]. 1 is the index so we take into consideration neighbour buckets.
sparsityVls = [1]

#the number of the bands 1-8
#range(1,9)
bandsVls = range(1,7)#,9)

#for every band value (bandsVls) there are 100 different band formations. John has filtered the better looking one for every band value, so now we only have about 5 band variations. (see datasets folder - see the names of the folders which contain the tables ex. 1_1_6)
# range(0,100)
bandsOffsetSeedVls = range(0,10)

#['none','BEA','BEARadius','TSPk','TSPkTransposedOnDefault','TSPkTransposedOnRearranged']
rearrangementPolicyVls = ['none', 'TSPk']#['none','BEA','BEARadius','TSPk','TSPkTransposedOnDefault']
rearrangementPolicyDefault = 'none'

numPartitionsModeVls = ['actual']# ['actual', 'likeDefault']

#The number of partitions (e.g. reducers, executors)
# [2,4,8,16,32,15,30]
# [10,20,40,80,160]
numPartitionsVls = [10,20,40,80]

#MBI is the default one
#['MBI','MBIREP','AICPM','AICPS', 'ACCPM', 'EPIC', 'EPCC','JB','M','WICRC']
#['MBI','AICPM','AICPS', 'ACCPM', 'EPIC', 'EPCC','JB','M','WICRC']
#good ones: MBI,AICPS,AICPM, EP, SRC
partitioningPolicyVls = ['MBI', 'AICPM']#['MBI', 'AICPM','AICPS', 'ACCPM', 'EPIC', 'EPCC','M','WICRC_1.0_1.0','WICRC_1.2_1.0','WICRC_1.5_1.0']#['MBI','AICPM','AICPS', 'ACCPM', 'EPIC', 'EPCC','M','WICRC_1.0_1.0','WICRC_1.2_1.0','WICRC_1.5_1.0']
partitioningPolicyDefault = 'MBI'

#['RANGE_SEARCH','BINARY_SEARCH']
searchPolicyVls = ['RANGE_SEARCH']
searchPolicyDefault = 'BINARY_SEARCH'

# For BINARY_SEARCH only : ['MAX_PARTITION_INPUT', 'MAX_PARTITION_CANDIDATE_CELLS']
binarySearchPolicyVls = ['MAX_PARTITION_INPUT', 'MAX_PARTITION_CANDIDATE_CELLS']
binarySearchPolicyDefault = 'MAX_PARTITION_INPUT'

# For RANGE_SEARCH only
rangeSearchPartitioningPolicyDefault = 'SAME' # 'SAME' or other ( 'MBI', 'MBIREP' etc.)
rangeSearchUpperBoundGranularity = '2.0_11'
# {max,min,mean,median,stdev,rep,imb}_{IC,CC}
# e.g. 'maxIC=0.3_imbIC=0.7-maxIC=0.5_maxCC=0.5'   2 sets of weights.
rangeSearchWeightsVls = [	'BJrepIC_1.0-BJmaxIC_0.0-BJmaxCC_0.0',
							'BJrepIC_0.0-BJmaxIC_1.0-BJmaxCC_0.0',
							'BJrepIC_0.0-BJmaxIC_0.0-BJmaxCC_1.0',
							'BJrepIC_0.5-BJmaxIC_0.0-BJmaxCC_0.5',
							'BJrepIC_0.33-BJmaxIC_0.33-BJmaxCC_0.33',
							'BJrepIC_0.25-BJmaxIC_0.25-BJmaxCC_0.5']
sortedRangeSearchWeightsVls = sortNormalizeWeights(rangeSearchWeightsVls) # To avoid duplications on PATHs and normalize weights (sum == 1.0). 
########################################################################
########################################################################
#
#	Part 2.1
#
#	Methods that are used from part 2.2 and write to files according to the execution mode.
#
########################################################################
# Real Partition Matrix - Parameters generation
def createParameters_RPM(buckets,sparsity,bands,bandsOffsetSeed,datasetDirectory, histogramsDirectory, foutRPM):
	"This method creates the parameters for the Real Partition Matrix execution mode in a form that is accepted by the system"
	params =  "executionMode=realPartitionMatrix"

	params += "\t" + "buckets="+str(buckets)
	params += "\t" + "sparsity=" + str(sparsity)
	params += "\t" + "bands=" + str(bands)
	params += "\t" + "bandsOffsetSeed=" + str(bandsOffsetSeed)
	
	params += "\t" + "datasetDirectory="+datasetDirectory
	params += "\t" + "histogramsDirectory="+ histogramsDirectory
	
	# Output
	params += "\t" + "partitionMatrixDirectory=" + datasetDirectory + "/" + str(buckets) + "/" + str(sparsity) + "_" + str(bands) + "_" + str(bandsOffsetSeed)
	
	#params += "\t" + "jobMaxExecutionHours=" + jobMaxExecutionHours
	
	#print(params)
	foutRPM.write(params + "\n")
########################################################################
# Rearrangements - Parameters generation
def createParameters_R(datasetDirectory, rearrangementPolicy,partitionMatrixDirectory,numPartitionsVls,tspkPath, foutR):
	"This method creates the parameters for the Rearrangements execution mode in a form that is accepted by the system"
	
	if not (os.path.exists(partitionMatrixDirectory)):
		return
		
	if rearrangementPolicy == "none":
		return
	
	# Input
        params = "executionMode=rearrangement"
	
	params += "\t" + "rearrangementPolicy="+rearrangementPolicy
	params += "\t" + "partitionMatrixDirectory="+partitionMatrixDirectory
	params += "\t" + "datasetDirectory=" + datasetDirectory
		
	if rearrangementPolicy == 'none':
		params += "\t" + "numPartitions=" + "-1"
		foutR.write(params + "\n")
	elif rearrangementPolicy == 'BEA':
		partitionMatrixRearrangedDirectory = partitionMatrixDirectory + "/" + rearrangementPolicy
		params += "\t" + "partitionMatrixRearrangedDirectory=" + partitionMatrixRearrangedDirectory
		params += "\t" + "numPartitions=" + "-1"
		foutR.write(params + "\n")
	elif rearrangementPolicy == 'BEARadius':
		partitionMatrixRearrangedDirectory = partitionMatrixDirectory + "/" + rearrangementPolicy + "_" + "3"
		
		params += "\t" + "partitionMatrixRearrangedDirectory=" + partitionMatrixRearrangedDirectory
		params += "\t" + "numPartitions=" + "-1"
		foutR.write(params + "\n")
	elif rearrangementPolicy.startswith('TSP'):
		for numPartitions in numPartitionsVls:
			partitionMatrixRearrangedDirectory = partitionMatrixDirectory + "/" + rearrangementPolicy + "_" + str(numPartitions)

			paramsTSP = params + "\t" + "numPartitions="+ str(numPartitions)
			paramsTSP += "\t" + "partitionMatrixRearrangedDirectory=" + partitionMatrixRearrangedDirectory
			paramsTSP += "\t" + "tspk=" + tspkPath
			foutR.write(paramsTSP + "\n")
########################################################################
def get_numPartitions_of_directory(dirpath):
	with open(dirpath + '/' + 'partitionsInputCost.csv', 'rt') as fin:
		lines = fin.readlines()
		return len(lines)
		
# Partitioning - Parameters generation
def createParameters_P_MBI(datasetDirectory, rearrangementPolicy,partitioningPolicy, searchPolicy, binarySearchPolicy, rangeSearchUpperBoundGranularity, rangeSearchWeights, #sortedRangeSearchWeightsVls, 
	numPartitions, numPartitionsMode, partitionMatrixDirectory, foutP):
	"This method creates the parameters for the Partitioning execution mode in a form that is accepted by the system"
	
	if rearrangementPolicy == 'none':
		partitionMatrixRearrangedDirectory = partitionMatrixDirectory
	elif rearrangementPolicy == 'BEA':
		partitionMatrixRearrangedDirectory = partitionMatrixDirectory + "/" + rearrangementPolicy
	elif rearrangementPolicy == 'BEARadius':
		partitionMatrixRearrangedDirectory = partitionMatrixDirectory + "/" + rearrangementPolicy + "_" + '3'
	elif rearrangementPolicy.startswith('TSPk'):
		partitionMatrixRearrangedDirectory = partitionMatrixDirectory + "/" + rearrangementPolicy + "_" + str(numPartitions)
	
	if not (os.path.exists(partitionMatrixRearrangedDirectory)):
		return

	prms = defaultdict()
	prms['executionMode'] = 'partitioning'
	prms['partitioningPolicy'] = partitioningPolicy
	prms['partitionMatrixDirectory'] = partitionMatrixRearrangedDirectory
	prms['searchPolicy'] = searchPolicy
	prms['binarySearchPolicy'] = binarySearchPolicy
	prms['datasetDirectory'] = datasetDirectory
	
	if rearrangementPolicy != 'none':
		prms['rearrangements'] = partitionMatrixRearrangedDirectory + "/" + "rearrangements.csv"
	
	# num partitions
	prms['numPartitions'] = str(numPartitions)
	
	defaultMbiPartitioningDirectory = partitionMatrixRearrangedDirectory + "/" + "MBI" + "_" + str(numPartitions) + "_" + 'actual'+ "/" + "BINARY_SEARCH" + "-" + binarySearchPolicyDefault
	if numPartitionsMode == 'likeDefault':
		# We need it to get the actual numReducers from it.
		if not os.path.exists(defaultMbiPartitioningDirectory):
			print("No defaultMbiPartitioningDirectory")
			return
		defaultPartitioningUsedPartitions = get_numPartitions_of_directory(defaultMbiPartitioningDirectory)
		prms['numPartitions'] = str(defaultPartitioningUsedPartitions)
	
	if searchPolicy == 'RANGE_SEARCH':
		prms['rangeSearchUpperBoundGranularity'] = rangeSearchUpperBoundGranularity
		prms['rangeSearchWeights'] = rangeSearchWeights #  "|".join(sortedRangeSearchWeightsVls)
	
	if numPartitionsMode == 'actual':
		partitioningDirectory = partitionMatrixRearrangedDirectory + "/" + partitioningPolicy + "_" + str(numPartitions) + "_" + numPartitionsMode + "/" + searchPolicy + "-" + binarySearchPolicy
		if searchPolicy == 'BINARY_SEARCH':
			prms['partitioningDirectory'] = partitioningDirectory
		elif searchPolicy == 'RANGE_SEARCH':
			prms['partitioningDirectory'] = partitioningDirectory + "-" + rangeSearchUpperBoundGranularity
			
			defaultPartitioningDirectory = partitionMatrixRearrangedDirectory + "/" + partitioningPolicy + "_" + str(numPartitions) + "_" + 'actual' + "/" + "BINARY_SEARCH" + "-" + binarySearchPolicy
			if not (os.path.exists(defaultPartitioningDirectory)):
				print("No defaultPartitioningDirectory")
				return
			
			prms['defaultPartitioningDirectory'] = defaultPartitioningDirectory
	elif numPartitionsMode == 'likeDefault':
		if searchPolicy == 'BINARY_SEARCH':
			prms['partitioningDirectory'] = partitionMatrixRearrangedDirectory + "/" + partitioningPolicy + "_" + str(defaultPartitioningUsedPartitions) + "_" + 'actual' + "/" + searchPolicy + "-" + binarySearchPolicy
		elif searchPolicy == 'RANGE_SEARCH':
			prms['partitioningDirectory'] = partitionMatrixRearrangedDirectory + "/" + partitioningPolicy + "_" + str(numPartitions) + "_" + 'likeDefault' + "/" + searchPolicy + "-" + binarySearchPolicy + "-" + rangeSearchUpperBoundGranularity
			
			defaultPartitioningDirectory = partitionMatrixRearrangedDirectory + "/" + partitioningPolicy + "_" + str(defaultPartitioningUsedPartitions) + "_" + 'actual' + "/" + "BINARY_SEARCH" + "-" + binarySearchPolicy
			if not (os.path.exists(defaultPartitioningDirectory)):
				print("No defaultPartitioningDirectory")
				return
			prms['defaultPartitioningDirectory'] = defaultPartitioningDirectory
	
	foutP.write("\t".join([x + "=" + prms[x] for x in sorted(prms.keys())]) + "\n")
	
########################################################################
########################################################################
#
#	Part 2.2
#
#	Now follows the actual execution of the program. Execution modes are checked and the main set of for-loops is
#	performed to generate the parameters according to the scripts' parameters' values. 
#
########################################################################


## Part 3.1. File creation and handlers. ##

foutRPM=None
foutR=None
foutP=None
foutMBI=None
# Real Partition Matrix
if realPartitionMatrix is True:
	realPartitionMatrixParametersFolder = parametersFolder + "/" + dataset
	if not os.path.exists(realPartitionMatrixParametersFolder):
		os.makedirs(realPartitionMatrixParametersFolder)
	realPartitionMatrixParametersFile = realPartitionMatrixParametersFolder + "/" + "realPartitionMatrix.txt"
	
	foutRPM = open(realPartitionMatrixParametersFile,'w')
	
# Rearrangement
if rearrangements is True:
	rearrangementsParametersFolder = parametersFolder + "/" + dataset
	if not os.path.exists(rearrangementsParametersFolder):
		os.makedirs(rearrangementsParametersFolder)
	rearrangementsParametersFile = rearrangementsParametersFolder + "/" + "rearrangements.txt"

	foutR = open(rearrangementsParametersFile,'w')
	
# Partitioning
if partitioning is True:
	partitioningParametersFolder = parametersFolder + "/" + dataset
	if not os.path.exists(partitioningParametersFolder):
		os.makedirs(partitioningParametersFolder)
	partitioningParametersFile = partitioningParametersFolder + "/" + "partitioning.txt"

	foutP = open(partitioningParametersFile,'w')
	
# MBucketI - MapReduce execution
if mbucketi is True:
	mBucketIParametersFolder = parametersFolder + "/" + dataset
	if not os.path.exists(mBucketIParametersFolder):
		os.makedirs(mBucketIParametersFolder)
	mBucketIParametersFile = mBucketIParametersFolder + "/" + "mBucketI.txt"

	foutMBI = open(mBucketIParametersFile,'w')

# So now we have at maximum four file handlers: foutRP, foutR, foutP, foutMBI

## Part 3.2. Main for-loop for the generation of the parameters. ##

# Path to the actual datatset
datasetDirectory = datasetsFolder+"/"+dataset

for numPartitionsMode in numPartitionsModeVls:
	for searchPolicy in searchPolicyVls:
		for buckets in bucketsVls:
			histogramsDirectory= datasetDirectory + "/" + str(buckets)
			for sparsity in sparsityVls:
				for bands in bandsVls:
					for bandsOffsetSeed in bandsOffsetSeedVls:
						partitionMatrixDirectory = histogramsDirectory + "/" + str(sparsity) + "_" + str(bands) + "_" + str(bandsOffsetSeed)
						
						#################################
						# Real Partition Matrix			#
						if realPartitionMatrix is True:##
							createParameters_RPM(buckets,sparsity,bands,bandsOffsetSeed,datasetDirectory,histogramsDirectory, foutRPM)
						
						for rearrangementPolicy in rearrangementPolicyVls:
							partitionMatrixDirectory = datasetDirectory + "/" + str(buckets) + "/" + str(sparsity) + "_" + str(bands) + "_" + str(bandsOffsetSeed)

							#############################
							# Rearrangements			#
							if rearrangements is True:###
								createParameters_R(datasetDirectory, rearrangementPolicy,partitionMatrixDirectory,numPartitionsVls,tspkPath, foutR)
							
								for partitioningPolicy in partitioningPolicyVls:
									for numPartitions in numPartitionsVls:
										for rangeSearchWeights in sortedRangeSearchWeightsVls:
											#############################
											# Partitioning & MBucketI	#
											if partitioning is True:
												for binarySearchPolicy in binarySearchPolicyVls:
													if partitioningPolicy.startswith("MBI") and  binarySearchPolicy != "MAX_PARTITION_INPUT":
														continue
													if searchPolicy == "BINARY_SEARCH":
														createParameters_P_MBI(datasetDirectory, rearrangementPolicy,partitioningPolicy, searchPolicy, binarySearchPolicy, 
															"None",['None'],
															numPartitions, numPartitionsMode, partitionMatrixDirectory, foutP)
													elif searchPolicy == "RANGE_SEARCH":
														createParameters_P_MBI(datasetDirectory, rearrangementPolicy,partitioningPolicy, searchPolicy, binarySearchPolicy, 
															rangeSearchUpperBoundGranularity, rangeSearchWeights,
															numPartitions, numPartitionsMode, partitionMatrixDirectory, foutP)

if realPartitionMatrix is True:
	foutRPM.close()
if rearrangements is True:
	foutR.close()
if partitioning is True:
	foutP.close()
if mbucketi is True:
	foutMBI.close()
