using MongoDB.Bson;
using MongoDB.Driver;

var client = new MongoClient("mongodb://_embedded_actually_does_not_matter");
var db = client.GetDatabase("test_db");

var col = db.GetCollection<Test>("test");

col.InsertOne(new Test { Nana = "SB" });


Console.ReadKey();

var documents = col.Find(_ => true).ToList();
foreach (var document in documents)
{
    Console.WriteLine(document);
}

record Test
{
    public ObjectId _id { get; set; }
    public string Nana { get; set; }
}
