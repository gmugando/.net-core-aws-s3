using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Threading.Tasks;
using Newtonsoft;
using Newtonsoft.Json;
using System.IO;

namespace S3CRUDOperation
{
     class Program
    {
        private const string AWS_ACCESS_KEY = "AKIAICLIWQ3TS6ULMCIQ";
        private const string AWS_SECRET_KEY = "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";
        private const string BUCKET_NAME = "test-bucket-kalam";
        private const string S3_KEY = "s3_key";
       public static async Task Main(string[] args)
        {
            var credentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY);
            AmazonS3Client client = new AmazonS3Client(credentials, RegionEndpoint.USEast2);


            
      


            //Create Bucket If Not Exists
            await CreateBucketAsync(client);

            //Write Books
            Console.WriteLine("Do you want to Save Book with Key Y/N...............................");
            var result = Console.ReadKey();
            Console.WriteLine("");
            if (result.KeyChar.ToString().ToLower() == "y")
            {
                Console.WriteLine("Please Enter is a key to Save Book with Dummy Data...............................");
                var bookKey = Console.ReadLine();
                await WriteBookOnS3Async(client, bookKey);
                Console.WriteLine("Book Data has been saved Sucessfully");

            }


            //Serach by Key
            Console.WriteLine("Do you want to Search Book with Key Y/N...............................");
            var searchbykeyresult = Console.ReadKey();
            Console.WriteLine("");
            if (searchbykeyresult.KeyChar.ToString().ToLower() =="y")
            {
                
                Console.WriteLine("Please Enter a Key...............................");
                var skey = Console.ReadLine();
                Console.WriteLine("Searching Book on S3 Bucket .......");
                await SearchS3ObjectAsync(client,skey);
            }



            Console.WriteLine("Do you want to Search Book with Book ID Y/N...............................");
            var serachByBookresult = Console.ReadKey();
            Console.WriteLine("");
            if (serachByBookresult.KeyChar.ToString().ToLower() == "y")
            {

                Console.WriteLine("Please Enter a Book ID...............................");
                var skey = Console.ReadLine();
                Console.WriteLine("Searching Book in S3 Bucket with Book Id.......");
                await SearchFromBucketWithBookIdAsync(client, skey);
            }








            Console.WriteLine("Do you want to Delete Book with Key Y/N...............................");
            result = Console.ReadKey();
            Console.WriteLine("");
            if (result.KeyChar.ToString().ToLower() == "y")
            {
                Console.WriteLine("Please Enter Key...............................");
                var skey = Console.ReadLine();
                Console.WriteLine("Deleting Book on S3 Bucket .......");
                await DeletS3Object(client,skey);

            }
            Console.WriteLine("Book has deleted SCuessfully");
        }

        private static async Task CreateBucketAsync(AmazonS3Client client)
        {
            Console.Out.WriteLine("Checking S3 bucket with name " + BUCKET_NAME);

            ListBucketsResponse response = await client.ListBucketsAsync();

            bool found = false;
            foreach (S3Bucket bucket in response.Buckets)
            {
                if (bucket.BucketName == BUCKET_NAME)
                {
                    Console.Out.WriteLine("Bucket already Exists...............................");
                    found = true;
                    break;
                }
            }

            if (found == false)
            {
                Console.Out.WriteLine("Bucket is Creating...............................");

                PutBucketRequest request = new PutBucketRequest();
                request.BucketName = BUCKET_NAME;

                await client.PutBucketAsync(request);

                Console.Out.WriteLine("Bucket has been Created S3 bucket with name " + BUCKET_NAME);
            }
        }
        private static async Task WriteBookOnS3Async(AmazonS3Client client,string key)
        {

            // Create a PutObject request
            PutObjectRequest request = new PutObjectRequest
            {
                BucketName = BUCKET_NAME,
                Key = key,
                ContentBody = GetBookObject()
            };

            // Put object
            PutObjectResponse response = await client.PutObjectAsync(request);


        }
        public static async Task<Book> SearchS3ObjectAsync(AmazonS3Client client, string key)
        {
            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = BUCKET_NAME,
                Key = key
            };

            GetObjectResponse response = await client.GetObjectAsync(request);

            StreamReader reader = new StreamReader(response.ResponseStream);

            string content = reader.ReadToEnd();
            if (!string.IsNullOrWhiteSpace(content))
            {
                //Console.WriteLine("Following Content Found.......");
                //Console.WriteLine(content);
                return JsonConvert.DeserializeObject<Book>(content);
            }
            else
                Console.WriteLine("No Data Found");
            return null;
        }
        private static async Task DeletS3Object(AmazonS3Client client,string key)
        {
            // Create a DeleteObject request
            DeleteObjectRequest request = new DeleteObjectRequest
            {
                BucketName = BUCKET_NAME,
                Key = key
            };

            // Issue request
           var result = await client.DeleteObjectAsync(request);

                Console.WriteLine("Book Has been removed Sucessfully");


        }

        public static async Task SearchFromBucketWithBookIdAsync(AmazonS3Client client, string bookId)
        {
            ListObjectsRequest req = new ListObjectsRequest {
                BucketName = BUCKET_NAME
            };

            ListObjectsResponse res = await client.ListObjectsAsync(req);
            foreach (S3Object obj in res.S3Objects)
            {
                var result = await SearchS3ObjectAsync(client, obj.Key).ConfigureAwait(true);

                if (result.bookid.ToString()== bookId.ToString())
                {
                    Console.WriteLine("Object Found with "+ bookId);
                    Console.WriteLine(JsonConvert.SerializeObject(result));
                }
            }
        }

        //Get Dummy Books Data
        private static string GetBookObject()
        {
            Book book = new Book();
            book.bookid = "12000";
            book.bookname = "Test Book Name";
            book.bookdescription = "This is a Test Book ";
            return JsonConvert.SerializeObject(book);
        }
    }
}
