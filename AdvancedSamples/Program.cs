namespace AdvancedSamples
{
    using Nest;
    using System;

    namespace PartialDocumentUpdateExample
    {
        class Program
        {
            static void Main(string[] args)
            {
                //Wire up an elastic client
                var elastic = new ElasticClient(
                    new ConnectionSettings(
                        new Uri("http://localhost:9200")));

                //Some useful constants
                const string IndexName = "indexName";
                const string Doc = "doc";

                //Source and destination indices
                var sourceIndex = "test_user_click";
                const string  Indices = "indices";
                const string IndexNameField = "indexName.keyword";

                //Process updates by each index 
                var indicesResponse = elastic.Search<dynamic>(s => s
                    .Size(0)
                    .Type(string.Empty)
                    .Index(sourceIndex)
                    .Aggregations(a => a
                        .Terms(Indices, at => at
                        .Field(IndexNameField)
                        .Size(100))));

                foreach (var item in indicesResponse.Aggregations.Terms(Indices).Buckets)
                {
                    var destinationIndex = item.Key;  //Set the destination index

                    //Scrolling information
                    var pageSize = 1000;
                    var scrollLifetime = "10s";
                    ISearchResponse<dynamic> searchResponse = null;

                    //scrolling thru the documents to update
                    while (true)
                    {
                        //perform a dynamic scrolling query against the source index
                        if (searchResponse == null)
                        {
                            searchResponse = elastic.Search<dynamic>(s => s
                                .Type(string.Empty)  //Ignore types completely
                                .Scroll(scrollLifetime)
                                .Index(sourceIndex) //Feature index to query
                                .Query(q => q
                                    .Match(m => m
                                        .Field(IndexName)
                                        .Query(destinationIndex)))  //Find features for the destination index
                                .Size(pageSize));
                        }
                        else
                        {
                            //Using scroll to get the remaining pages of feature documents
                            searchResponse = elastic.Scroll<dynamic>(
                                new ScrollRequest(searchResponse.ScrollId, scrollLifetime));
                        }

                        //have we reached the end
                        if (searchResponse.Documents.Count == 0)
                            break;

                        //create new bulk descriptior to hold the list of updates
                        var bulkDescriptor = new BulkDescriptor();
                        bulkDescriptor.Index(destinationIndex);
                        bulkDescriptor.Type(Doc);

                        //create a partial update for the search record
                        //set the current userClickBoost for each boosted document
                        foreach (var analyticDoc in searchResponse.Documents)
                        {
                            bulkDescriptor.Update<dynamic, PartialDoc>(s => s
                            .Id((string)analyticDoc.sourceId)
                            .Doc(
                                new PartialDoc {
                                    UserClickBoost = (double)analyticDoc.userClickBoost
                                    
                                    // additional feature boosts here
                                }));
                        }

                        //Send the bulk request to destination index
                        var updateResponse = elastic.Bulk(bulkDescriptor);

                        if (updateResponse.Errors)
                        {
                            //Handle errors if needed
                        }
                    }
                }
            }
        }
    }
}


