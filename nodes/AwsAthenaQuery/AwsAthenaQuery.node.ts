import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { NodeConnectionType, NodeOperationError, NodeApiError } from 'n8n-workflow';
import { sign } from 'aws4';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Helper function to make Athena API requests
async function makeAthenaRequest(
	executeFunctions: IExecuteFunctions,
	region: string,
	target: string,
	payload: any,
	credentials: any,
): Promise<any> {
	const endpoint = `https://athena.${region}.amazonaws.com/`;
	const payloadString = JSON.stringify(payload);

	// Construct request options for aws4 signing
	const requestOptions = {
		host: `athena.${region}.amazonaws.com`,
		method: 'POST',
		path: '/',
		body: payloadString,
		region,
		service: 'athena',
		headers: {
			'Content-Type': 'application/x-amz-json-1.1',
			'X-Amz-Target': target,
		},
	};

	// Use aws4 to sign the request
	sign(requestOptions, {
		accessKeyId: credentials.accessKeyId,
		secretAccessKey: credentials.secretAccessKey,
		sessionToken: credentials.sessionToken,
	});

	try {
		const response = await executeFunctions.helpers.httpRequest({
			method: 'POST',
			url: endpoint,
			headers: requestOptions.headers,
			body: payloadString,
			json: true,
		});
		return response;
	} catch (error: any) {
		// Enhanced error handling for AWS API responses
		let errorMessage = 'AWS Athena API request failed';
		let statusCode = 'Unknown';
		let awsErrorCode = 'Unknown';
		let awsErrorMessage = 'Unknown';
		let responseBody = 'No response body';

		if (error.response) {
			statusCode = error.response.statusCode || error.response.status || 'Unknown';
			responseBody = error.response.body || error.response.data || 'No response body';
			
			// Try to parse AWS error response in multiple formats
			try {
				let errorBodyStr = '';
				if (typeof responseBody === 'string') {
					errorBodyStr = responseBody;
				} else if (typeof responseBody === 'object') {
					errorBodyStr = JSON.stringify(responseBody);
				} else {
					errorBodyStr = String(responseBody);
				}
				
				if (errorBodyStr) {
					// Try parsing as JSON first
					try {
						const awsError = JSON.parse(errorBodyStr);
						awsErrorCode = awsError.__type || awsError.Code || awsError.code || 'Unknown';
						awsErrorMessage = awsError.message || awsError.Message || awsError.msg || errorBodyStr;
					} catch (jsonParseError) {
						// If JSON parsing fails, check if it's an XML response
						if (errorBodyStr.includes('<')) {
							// Basic XML parsing for AWS errors
							const codeMatch = errorBodyStr.match(/<Code>([^<]+)<\/Code>/);
							const messageMatch = errorBodyStr.match(/<Message>([^<]+)<\/Message>/);
							awsErrorCode = codeMatch ? codeMatch[1] : 'XMLParseError';
							awsErrorMessage = messageMatch ? messageMatch[1] : errorBodyStr;
						} else {
							awsErrorMessage = errorBodyStr;
						}
					}
				}
			} catch (parseError) {
				awsErrorMessage = `Parse error: ${parseError.message}`;
			}
			
			errorMessage = `AWS Athena API Error (${statusCode}): ${awsErrorCode} - ${awsErrorMessage}`;
		} else if (error.message) {
			errorMessage = `Request Error: ${error.message}`;
		}

		// Add comprehensive debugging information
		const debugInfo = {
			endpoint,
			target,
			payload: payload,
			region,
			statusCode,
			awsErrorCode,
			awsErrorMessage,
			responseBody,
			requestHeaders: requestOptions.headers,
			originalError: error.message,
			errorType: error.constructor.name,
		};

		if (error.response) {
			// This is an API error from AWS - use NodeApiError
			throw new NodeApiError(executeFunctions.getNode(), error, {
				message: `${errorMessage}\nDebug Info: ${JSON.stringify(debugInfo, null, 2)}`,
			});
		} else {
			// This is a general operation error - use NodeOperationError
			throw new NodeOperationError(
				executeFunctions.getNode(),
				`${errorMessage}\nDebug Info: ${JSON.stringify(debugInfo, null, 2)}`,
			);
		}
	}
}

/**
 * Get temporary credentials from AWS environment following the AWS credential chain.
 * Attempts to get credentials in the following order:
 * 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
 * 2. EKS Pod Identity (AWS_CONTAINER_CREDENTIALS_FULL_URI)
 * 3. ECS/Fargate container metadata (AWS_CONTAINER_CREDENTIALS_RELATIVE_URI)
 * 4. EC2 instance metadata service (IMDSv2/IMDSv1)
 *
 * Based on n8n's system-credentials-utils.ts implementation
 */
async function getSystemCredentials(): Promise<{
	accessKeyId: string;
	secretAccessKey: string;
	sessionToken?: string;
}> {
	// 1. Try environment variables
	const envCreds = getEnvironmentCredentials();
	if (envCreds) return envCreds;

	// 2. Try EKS Pod Identity
	const podCreds = await getPodIdentityCredentials();
	if (podCreds) return podCreds;

	// 3. Try ECS/Fargate container metadata
	const containerCreds = await getContainerMetadataCredentials();
	if (containerCreds) return containerCreds;

	// 4. Try EC2 instance metadata
	const instanceCreds = await getInstanceMetadataCredentials();
	if (instanceCreds) return instanceCreds;

	throw new Error(
		'No AWS credentials found. Ensure running in AWS (EC2/ECS/EKS) with IAM role or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.',
	);
}

function getEnvironmentCredentials() {
	const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
	const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
	const sessionToken = process.env.AWS_SESSION_TOKEN;

	if (accessKeyId && secretAccessKey) {
		return {
			accessKeyId: accessKeyId.trim(),
			secretAccessKey: secretAccessKey.trim(),
			sessionToken: sessionToken?.trim(),
		};
	}

	return null;
}

async function getPodIdentityCredentials() {
	const fullUri = process.env.AWS_CONTAINER_CREDENTIALS_FULL_URI;
	if (!fullUri) return null;

	try {
		const authToken = process.env.AWS_CONTAINER_AUTHORIZATION_TOKEN;
		const headers: Record<string, string> = { 'User-Agent': 'n8n-aws-athena' };

		if (authToken) {
			headers.Authorization = `Bearer ${authToken}`;
		}

		const response = await fetch(fullUri, {
			method: 'GET',
			headers,
			signal: AbortSignal.timeout(2000),
		});

		if (!response.ok) return null;

		const data = await response.json() as { AccessKeyId: string; SecretAccessKey: string; Token: string };
		return {
			accessKeyId: data.AccessKeyId,
			secretAccessKey: data.SecretAccessKey,
			sessionToken: data.Token,
		};
	} catch {
		return null;
	}
}

async function getContainerMetadataCredentials() {
	const relativeUri = process.env.AWS_CONTAINER_CREDENTIALS_RELATIVE_URI;
	if (!relativeUri) return null;

	try {
		const authToken = process.env.AWS_CONTAINER_AUTHORIZATION_TOKEN;
		const headers: Record<string, string> = { 'User-Agent': 'n8n-aws-athena' };

		if (authToken) {
			headers.Authorization = `Bearer ${authToken}`;
		}

		const response = await fetch(`http://169.254.170.2${relativeUri}`, {
			method: 'GET',
			headers,
			signal: AbortSignal.timeout(2000),
		});

		if (!response.ok) return null;

		const data = await response.json() as { AccessKeyId: string; SecretAccessKey: string; Token: string };
		return {
			accessKeyId: data.AccessKeyId,
			secretAccessKey: data.SecretAccessKey,
			sessionToken: data.Token,
		};
	} catch {
		return null;
	}
}

async function getInstanceMetadataCredentials() {
	try {
		const baseUrl = 'http://169.254.169.254/latest';
		const headers: Record<string, string> = { 'User-Agent': 'n8n-aws-athena' };

		// Try to obtain an IMDSv2 token
		try {
			const tokenResponse = await fetch(`${baseUrl}/api/token`, {
				method: 'PUT',
				headers: {
					'X-aws-ec2-metadata-token-ttl-seconds': '21600',
					'User-Agent': 'n8n-aws-athena',
				},
				signal: AbortSignal.timeout(2000),
			});

			if (tokenResponse.ok) {
				const token = await tokenResponse.text();
				headers['X-aws-ec2-metadata-token'] = token;
			}
		} catch {
			// IMDSv2 may be disabled; continue with IMDSv1
		}

		const roleResponse = await fetch(`${baseUrl}/meta-data/iam/security-credentials/`, {
			method: 'GET',
			headers,
			signal: AbortSignal.timeout(2000),
		});

		if (!roleResponse.ok) return null;

		const roleName = (await roleResponse.text()).trim();
		if (!roleName) return null;

		const credsResponse = await fetch(
			`${baseUrl}/meta-data/iam/security-credentials/${roleName}`,
			{
				method: 'GET',
				headers,
				signal: AbortSignal.timeout(2000),
			},
		);

		if (!credsResponse.ok) return null;

		const data = await credsResponse.json() as { AccessKeyId?: string; SecretAccessKey?: string; Token?: string };
		if (!data?.AccessKeyId || !data?.SecretAccessKey) return null;

		return {
			accessKeyId: data.AccessKeyId,
			secretAccessKey: data.SecretAccessKey,
			sessionToken: data.Token,
		};
	} catch {
		return null;
	}
}

/**
 * Call STS AssumeRole API to get temporary credentials
 */
async function callStsAssumeRole(
	executeFunctions: IExecuteFunctions,
	stsAccessKeyId: string,
	stsSecretAccessKey: string,
	stsSessionToken: string | undefined,
	roleArn: string,
	externalId: string,
	roleSessionName: string,
	region: string,
): Promise<{ accessKeyId: string; secretAccessKey: string; sessionToken: string }> {
	const params = new URLSearchParams({
		Action: 'AssumeRole',
		Version: '2011-06-15',
		RoleArn: roleArn,
		RoleSessionName: roleSessionName,
	});

	if (externalId?.trim()) {
		params.append('ExternalId', externalId);
	}

	const domain = region.startsWith('cn-') ? 'amazonaws.com.cn' : 'amazonaws.com';
	const stsEndpoint = `https://sts.${region}.${domain}/`;
	const body = params.toString();

	// Construct request options for aws4 signing
	const requestOptions = {
		host: `sts.${region}.${domain}`,
		method: 'POST',
		path: '/',
		body,
		region,
		service: 'sts',
		headers: {
			'Content-Type': 'application/x-www-form-urlencoded',
		},
	};

	// Use aws4 to sign the request
	sign(requestOptions, {
		accessKeyId: stsAccessKeyId,
		secretAccessKey: stsSecretAccessKey,
		sessionToken: stsSessionToken,
	});

	try {
		const response = (await executeFunctions.helpers.httpRequest({
			method: 'POST',
			url: stsEndpoint,
			headers: requestOptions.headers,
			body,
		})) as any;

		const credentials = response.AssumeRoleResponse?.AssumeRoleResult?.Credentials;

		if (!credentials?.AccessKeyId || !credentials?.SecretAccessKey || !credentials?.SessionToken) {
			throw new NodeOperationError(
				executeFunctions.getNode(),
				'Invalid STS response: missing credentials',
			);
		}

		return {
			accessKeyId: credentials.AccessKeyId,
			secretAccessKey: credentials.SecretAccessKey,
			sessionToken: credentials.SessionToken,
		};
	} catch (error: any) {
		const awsError =
			error.response?.body?.ErrorResponse?.Error || error.response?.data?.ErrorResponse?.Error;
		const errorMessage = awsError
			? `${awsError.Code}: ${awsError.Message}`
			: error.message || 'Unknown error';

		throw new NodeOperationError(
			executeFunctions.getNode(),
			`STS AssumeRole failed: ${errorMessage}`,
		);
	}
}

/**
 * Get AWS credentials based on the authentication method selected in node parameters
 */
async function getAwsCredentials(
	executeFunctions: IExecuteFunctions,
	authMethod: string,
	region: string,
): Promise<{ accessKeyId: string; secretAccessKey: string; sessionToken?: string }> {
	if (authMethod === 'standardCredentials') {
		// Use standard AWS credentials
		const creds = await executeFunctions.getCredentials('aws');
		return {
			accessKeyId: creds.accessKeyId as string,
			secretAccessKey: creds.secretAccessKey as string,
			sessionToken: creds.sessionToken as string | undefined,
		};
	}

	// Use AssumeRole to get temporary credentials
	const creds = await executeFunctions.getCredentials('awsAssumeRole');

	const { accessKeyId, secretAccessKey, sessionToken } = creds.useSystemCredentialsForRole
		? await getSystemCredentials()
		: {
				accessKeyId: creds.stsAccessKeyId as string,
				secretAccessKey: creds.stsSecretAccessKey as string,
				sessionToken: creds.stsSessionToken as string | undefined,
			};

	return await callStsAssumeRole(
		executeFunctions,
		accessKeyId,
		secretAccessKey,
		sessionToken,
		creds.roleArn as string,
		(creds.externalId as string) || '',
		(creds.roleSessionName as string) || 'n8n-athena',
		region,
	);
}

export class AwsAthenaQuery implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'AWS Athena Query',
		name: 'awsAthenaQuery',
		icon: 'file:AwsAthenaQuery.node.svg',
		group: ['transform'],
		version: 1,
		description: 'Execute SQL queries on AWS Athena',
		defaults: {
			name: 'AWS Athena Query',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main],
		usableAsTool: true,
		credentials: [
			{
				name: 'aws',
				required: true,
				displayOptions: {
					show: {
						authMethod: ['standardCredentials'],
					},
				},
			},
			{
				name: 'awsAssumeRole',
				required: true,
				displayOptions: {
					show: {
						authMethod: ['assumeRole'],
					},
				},
			},
		],
		properties: [
			{
				displayName: 'Authentication Method',
				name: 'authMethod',
				type: 'options',
				options: [
					{
						name: 'Standard Credentials',
						value: 'standardCredentials',
						description: 'Use AWS IAM Access Key and Secret Key',
					},
					{
						name: 'AssumeRole',
						value: 'assumeRole',
						description: 'Use STS AssumeRole for temporary credentials',
					},
				],
				default: 'assumeRole',
			},
			{
				displayName: 'Region',
				name: 'region',
				type: 'string',
				default: 'us-east-1',
				placeholder: 'us-east-1',
				description: 'AWS region where your Athena service is located',
				required: true,
			},
			{
				displayName: 'Database Name',
				name: 'database',
				type: 'string',
				default: '',
				placeholder: 'Optional',
				description: 'Name of the database to query. Leave empty to use the default database.',
			},
			{
				displayName: 'SQL Query',
				name: 'query',
				type: 'string',
				default: '',
				noDataExpression: false,
				required: true,
				typeOptions: {
					editor: 'sqlEditor',
					rows: 5,
				},
				placeholder: 'SELECT * FROM my_table LIMIT 10',
				description: 'The SQL query to execute',
			},
			{
				displayName: 'S3 Output Location',
				name: 's3OutputLocation',
				type: 'string',
				default: '',
				placeholder: 's3://my-bucket/athena-results/',
				description: 'S3 bucket path where Athena will save query results',
				required: true,
			},
			{
				displayName: 'Query Timeout (Seconds)',
				name: 'timeout',
				type: 'number',
				default: 300,
				description: 'Maximum time to wait for query completion. Defaults to 300 seconds.',
				required: true,
			},
			{
				displayName: 'Output Format',
				name: 'outputFormat',
				type: 'options',
				options: [
					{
						name: 'Table Format',
						value: 'tableFormat',
						description:
							'Each database row becomes a separate workflow item (best for data processing)',
					},
					{
						name: 'Raw Format',
						value: 'rawFormat',
						description:
							'All results in one item with additional metadata (query ID, row count, etc.)',
					},
				],
				default: 'tableFormat',
				description: 'How to structure the query results for use in your workflow',
				required: true,
			},
			{
				displayName: 'Max Rows Returned',
				name: 'maxRowsMode',
				type: 'options',
				options: [
					{
						name: 'No Limit',
						value: 'noLimit',
						description: 'Return all available rows (Warning: May be slow)',
					},
					{
						name: 'Limit Applied',
						value: 'limitApplied',
						description: 'Return up to a maximum number of rows',
					},
				],
				default: 'noLimit',
				description: 'Control how many rows are returned from the query',
				required: true,
			},
			{
				displayName: 'Max Rows',
				name: 'maxRows',
				type: 'number',
				default: 10000,
				description: 'Maximum number of rows to return when limit is applied',
				required: true,
				displayOptions: {
					show: {
						maxRowsMode: ['limitApplied'],
					},
				},
				typeOptions: {
					minValue: 1,
				},
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const resultItems: INodeExecutionData[] = [];

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			try {
				// Get node parameters
				const authMethod = this.getNodeParameter('authMethod', itemIndex) as string;
				const region = this.getNodeParameter('region', itemIndex) as string;
				const database = this.getNodeParameter('database', itemIndex) as string;
				const query = this.getNodeParameter('query', itemIndex) as string;
				const s3OutputLocation = this.getNodeParameter('s3OutputLocation', itemIndex) as string;
				const outputFormat = this.getNodeParameter(
					'outputFormat',
					itemIndex,
					'tableFormat',
				) as string;
				const timeout = this.getNodeParameter('timeout', itemIndex, 300) as number;

				// Validate required parameters
				if (!region || !region.trim()) {
					throw new NodeOperationError(this.getNode(), 'Region is required.');
				}
				if (!query || !query.trim()) {
					throw new NodeOperationError(this.getNode(), 'SQL Query is required.');
				}
				if (!s3OutputLocation || !s3OutputLocation.trim()) {
					throw new NodeOperationError(this.getNode(), 'S3 Output Location is required.');
				}
				if (timeout <= 0) {
					throw new NodeOperationError(this.getNode(), 'Timeout must be greater than 0.');
				}

				// Get AWS credentials based on selected authentication method
				const credentials = await getAwsCredentials(this, authMethod, region);

				// Generate a unique client request token for idempotency (minimum 32 characters required)
				const timestamp = Date.now().toString();
				const randomPart =
					Math.random().toString(36).substring(2, 17) + Math.random().toString(36).substring(2, 17);
				const clientRequestToken = `n8n-${timestamp}-${randomPart}`.substring(0, 64); // Ensure it's at least 32 chars, max 64

				// Prepare query execution parameters
				const queryParams: any = {
					QueryString: query,
					ClientRequestToken: clientRequestToken,
					ResultConfiguration: {
						OutputLocation: s3OutputLocation,
					},
				};

				// Add database context if provided
				if (database && database.trim() !== '') {
					queryParams.QueryExecutionContext = {
						Database: database,
					};
				}

				// Read row limit settings
				const maxRowsMode = this.getNodeParameter('maxRowsMode', itemIndex, 'noLimit') as string;
				const maxRowsValue =
					maxRowsMode === 'limitApplied'
						? (this.getNodeParameter('maxRows', itemIndex) as number)
						: undefined;

				if (maxRowsMode === 'limitApplied') {
					if (!Number.isInteger(maxRowsValue) || (maxRowsValue as number) <= 0) {
						throw new NodeOperationError(this.getNode(), 'Max Rows must be a positive integer.');
					}
				}

				// Start query execution
				const startResponse = await makeAthenaRequest(
					this,
					region,
					'AmazonAthena.StartQueryExecution',
					queryParams,
					credentials,
				);

				const queryExecutionId = startResponse.QueryExecutionId;

				if (!queryExecutionId) {
					throw new NodeOperationError(this.getNode(), 'Failed to start Athena query execution.');
				}

				// Wait for query completion
				let queryStatus = 'RUNNING';
				const startTime = Date.now();
				const timeoutMs = timeout * 1000;

				while (queryStatus === 'RUNNING' || queryStatus === 'QUEUED') {
					// Check timeout
					if (Date.now() - startTime > timeoutMs) {
						throw new NodeOperationError(
							this.getNode(),
							`Query timed out after ${timeout} seconds.`,
						);
					}

					// Wait before checking status again
					await sleep(2000); // Wait 2 seconds

					const statusResponse = await makeAthenaRequest(
						this,
						region,
						'AmazonAthena.GetQueryExecution',
						{ QueryExecutionId: queryExecutionId },
						credentials,
					);

					queryStatus = statusResponse.QueryExecution?.Status?.State || 'FAILED';

					if (queryStatus === 'FAILED' || queryStatus === 'CANCELLED') {
						const reason =
							statusResponse.QueryExecution?.Status?.StateChangeReason || 'Unknown error';
						throw new NodeOperationError(
							this.getNode(),
							`Query failed or was cancelled: ${reason}`,
						);
					}

					// Handle unexpected query states
					if (
						queryStatus !== 'RUNNING' &&
						queryStatus !== 'QUEUED' &&
						queryStatus !== 'SUCCEEDED'
					) {
						throw new NodeOperationError(
							this.getNode(),
							`Query ended with unexpected status: ${queryStatus}`,
						);
					}
				}

				// Get query results with pagination (Athena returns up to 1000 rows per page including a header row on the first page)
				let nextToken: string | undefined = undefined;
				let pageIndex = 0;
				let columns: string[] = [];
				const parsedResults: Array<{ [key: string]: any }> = [];

				do {
					const payload: any = { QueryExecutionId: queryExecutionId, MaxResults: 1000 };
					if (nextToken) payload.NextToken = nextToken;

					const resultsResponse = await makeAthenaRequest(
						this,
						region,
						'AmazonAthena.GetQueryResults',
						payload,
						credentials,
					);

					const rows = resultsResponse.ResultSet?.Rows || [];

					if (rows.length === 0) {
						nextToken = resultsResponse.NextToken;
						pageIndex += 1;
						continue;
					}

					// On first page, extract header row as column names
					let dataRows: any[] = rows;
					if (pageIndex === 0) {
						columns = rows[0]?.Data?.map((data: any) => data.VarCharValue || '') || [];
						dataRows = rows.slice(1); // skip header on first page
					}

					for (const row of dataRows) {
						const rowData = row.Data || [];
						const parsedRow: { [key: string]: any } = {};

						columns.forEach((column: any, index: number) => {
							if (column) {
								const cellData = rowData[index];
								let value = null;
								if (cellData) {
									value =
										cellData.VarCharValue ||
										cellData.BigIntValue ||
										cellData.BooleanValue ||
										cellData.DateValue ||
										cellData.DoubleValue ||
										cellData.FloatValue ||
										cellData.IntegerValue ||
										cellData.TimestampValue ||
										null;
								}
								parsedRow[column] = value;
							}
						});

						parsedResults.push(parsedRow);
						if (
							maxRowsMode === 'limitApplied' &&
							parsedResults.length >= (maxRowsValue as number)
						) {
							// Stop collecting more rows; break out after current page
							break;
						}
					}

					// If we've reached the max rows, stop pagination
					if (maxRowsMode === 'limitApplied' && parsedResults.length >= (maxRowsValue as number)) {
						nextToken = undefined;
					} else {
						nextToken = resultsResponse.NextToken;
					}
					pageIndex += 1;
				} while (nextToken);

				// If no data rows were returned
				if (parsedResults.length === 0) {
					if (outputFormat === 'rawFormat') {
						resultItems.push({
							json: {
								queryExecutionId,
								rowCount: 0,
								columns: [],
								results: [],
							},
						});
					}
					continue;
				}

				// Add results to output based on format
				if (outputFormat === 'tableFormat') {
					for (const row of parsedResults) {
						resultItems.push({ json: row });
					}
				} else {
					resultItems.push({
						json: {
							queryExecutionId,
							rowCount: parsedResults.length,
							columns,
							results: parsedResults,
						},
					});
				}
			} catch (error: any) {
				if (this.continueOnFail()) {
					resultItems.push({
						json: this.getInputData(itemIndex)[0].json,
						error,
						pairedItem: itemIndex,
					});
				} else {
					throw new NodeOperationError(this.getNode(), error, {
						itemIndex,
					});
				}
			}
		}

		return [resultItems];
	}
}