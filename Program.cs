using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace AWSSync
{
	internal static class ProgramExtensions
	{
		public static IEnumerable<S3Object> ListAllObjects(this AmazonS3Client client, string bucketName, string prefix )
		{
			var files = new List<S3Object>();

			var request = new ListObjectsRequest
			{
				BucketName = bucketName,
				Prefix = prefix
			};

			while (true)
			{
				var response = client.ListObjects(request);

				files.AddRange(response.S3Objects);

				if (!response.IsTruncated)
				{
					break;
				}

				request.Marker = response.NextMarker;
			}

			return files;
		}
	}

	internal static class Program
	{
		private static void Main(string[] args)
		{
			if (args.Length != 6)
			{
				Console.WriteLine("Args: <directory> <profileName> <region> <bucket> <prefix> <# of concurrent uploads>");
				return;
			}

			var directory = args[0];
			var profileName = args[1];
			var region = args[2];
			var bucketName = args[3];
			var prefix = args[4];
			var maxConcurrentUploads = int.Parse(args[5]);
			var credentials = new StoredProfileAWSCredentials(profileName);

			using (var client = new AmazonS3Client(credentials, RegionEndpoint.GetBySystemName(region)))
			{
				var s3files = client.ListAllObjects(bucketName, prefix);
				var localfiles = Directory.EnumerateFiles(directory, "*.*", SearchOption.AllDirectories);

				var uploadRequests = (
					from local in localfiles
					let remoteKey = prefix + "/" + RemoveFilePrefix(local, directory).Replace('\\', '/')
					join s3 in s3files on remoteKey equals s3.Key into matches
					let localInfo = new FileInfo(local)
					let match = matches.FirstOrDefault()
					where match == null || match.Size != localInfo.Length || match.LastModified < localInfo.LastWriteTime
					select new PutObjectRequest
					{
						BucketName = bucketName,
						Key = remoteKey,
						FilePath = local,
					}).ToList();

				if (uploadRequests.Count == 0)
				{
					Console.WriteLine("Everything is up to date.");
					return;
				}

				Console.WriteLine("Uploading {0} files", uploadRequests.Count);
				Parallel.ForEach(uploadRequests, new ParallelOptions { MaxDegreeOfParallelism = maxConcurrentUploads }, request => {
					// ReSharper disable once AccessToDisposedClosure - Parallel.ForEach will run before client is disposed
					client.PutObject(request);
					Console.WriteLine("{0} uploaded.", request.Key);
				});
				Console.WriteLine("done.");
			}

		}

		private static string RemoveFilePrefix(string file, string prefix)
		{
			if (!file.StartsWith(prefix) || file.Length <= prefix.Length)
			{
				throw new Exception();
			}

			var position = prefix.Length;
			if (file[position] == '\\' || file[position] == '/')
			{
				position++;
			}

			return file.Substring(position);
		}
	}
}
