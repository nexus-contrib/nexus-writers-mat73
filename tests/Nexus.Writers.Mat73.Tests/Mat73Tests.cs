using HDF5.NET;
using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Runtime.InteropServices;
using Xunit;

namespace Nexus.Writers.Tests;

public class Mat73Tests(DataWriterFixture fixture) : IClassFixture<DataWriterFixture>
{
    private readonly DataWriterFixture _fixture = fixture;

    [Theory]
    [InlineData(Precision.Float32)]
    [InlineData(Precision.Float64)]
    public async Task CanWriteFiles(Precision precision)
    {
        var targetFolder = _fixture.GetTargetFolder();
        var dataWriter = new Mat73() as IDataWriter;

        var context = new DataWriterContext(
            ResourceLocator: new Uri(targetFolder),
            Precision: precision,
            RequestConfiguration: default!
        );

        await dataWriter.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var begin = new DateTime(2020, 01, 01, 0, 0, 0, DateTimeKind.Utc);
        var samplePeriod = TimeSpan.FromSeconds(1);

        var catalogItems = _fixture.Catalogs.SelectMany(catalog => catalog.Resources!
            .SelectMany(resource => resource.Representations!.Select(representation => new CatalogItem(catalog, resource, representation, default))))
            .ToArray();

        var random = new Random(Seed: 1);

        var length = 1000;

        var data = new[]
        {
            Enumerable
                .Range(0, length)
                .Select(value => random.NextDouble() * 1e4)
                .ToArray(),

            Enumerable
                .Range(0, length)
                .Select(value => random.NextDouble() * -1)
                .ToArray(),

            Enumerable
                .Range(0, length)
                .Select(value => random.NextDouble() * Math.PI)
                .ToArray()
        };

        var requests = CreateRequests(catalogItems, data, precision);

        await dataWriter.OpenAsync(begin, TimeSpan.FromSeconds(2000), samplePeriod, catalogItems, CancellationToken.None);
        await dataWriter.WriteAsync(TimeSpan.Zero, requests, new Progress<double>(), CancellationToken.None);
        await dataWriter.WriteAsync(TimeSpan.FromSeconds(length), requests, new Progress<double>(), CancellationToken.None);
        await dataWriter.CloseAsync(CancellationToken.None);

        var actualFilePaths = Directory
            .GetFiles(targetFolder)
            .OrderBy(value => value)
            .ToArray();

        // assert
        Assert.Single(actualFilePaths);

        using var h5File = H5File.OpenRead(actualFilePaths.First());

        Assert.Equal(4, h5File.Children.Count());

        var properties = h5File.Group("properties");

        // catalog 1
        var catalog1 = h5File.Group("A_B_C");
        var resources1 = _fixture.Catalogs[0].Resources;
        Assert.Equal(resources1!.Count + 1, catalog1.Children.Count());
        var representations1 = _fixture.Catalogs[0].Resources!.SelectMany(resource => resource.Representations!).ToList();
        Assert.Equal(representations1.Count, catalog1.Group("resource1").Children.Count());
        // implement when HDF5.NET is able to read any struct easily
        //AssertProperties(_fixture.Catalogs[0].Properties, catalog1.Properties);
        //AssertProperties(_fixture.Catalogs[0].Resources[0].Properties, catalog1.Resources[0].Properties);
        //AssertProperties(_fixture.Catalogs[0].Resources[0].Properties, catalog1.Resources[1].Properties);

        AssertData(catalog1.Group("resource1").Dataset("dataset_1_s"), data[0], precision);

        AssertData(catalog1.Group("resource1").Dataset("dataset_2_s"), data[1], precision);

        // catalog 2
        var catalog2 = h5File.Group("D_E_F");
        var resources2 = _fixture.Catalogs[0].Resources;
        Assert.Equal(resources2!.Count + 1, catalog2.Children.Count());
        var representations2 = _fixture.Catalogs[1].Resources!.SelectMany(resource => resource.Representations!).ToList();
        Assert.Equal(representations2.Count, catalog2.Group("resource3").Children.Count());
        // implement when HDF5.NET is able to read any struct easily
        //AssertProperties(_fixture.Catalogs[1].Properties, catalog2.Properties);
        //AssertProperties(_fixture.Catalogs[1].Resources[0].Properties, catalog2.Resources[0].Properties);

        AssertData(catalog2.Group("resource3").Dataset("dataset_1_s"), data[2], precision);

        //void AssertProperties(IReadOnlyDictionary<string, string> expected, List<xxx> actual)
        //{
        //    Assert.Equal(expected.Count, actual.Count);

        //    for (int i = 0; i < actual.Count; i++)
        //    {
        //        var key = actual[i].Name;
        //        var value = actual[i].Value;

        //        var match = expected[key];
        //        Assert.Equal(match, value);
        //    }
        //}

        static WriteRequest[] CreateRequests(CatalogItem[] catalogItems, double[][] data, Precision precision)
        {
            return precision switch
            {
                Precision.Float32 => catalogItems
                    .Select((catalogItem, i) => new WriteRequest(catalogItem, MemoryMarshal.Cast<float, byte>(data[i].Select(value => (float)value).ToArray()).ToArray()))
                    .ToArray(),

                Precision.Float64 => catalogItems
                    .Select((catalogItem, i) => new WriteRequest(catalogItem, MemoryMarshal.Cast<double, byte>(data[i]).ToArray()))
                    .ToArray(),

                _ => throw new NotSupportedException($"The precision {precision} is not supported.")
            };
        }

        static void AssertData(H5Dataset dataset, double[] expected, Precision precision)
        {
            switch (precision)
            {
                case Precision.Float32:
                    var expectedSingle = expected.Select(value => (float)value).ToArray();
                    Assert.True(expectedSingle.Concat(expectedSingle).SequenceEqual(dataset.Read<float>()));
                    break;

                case Precision.Float64:
                    Assert.True(expected.Concat(expected).SequenceEqual(dataset.Read<double>()));
                    break;

                default:
                    throw new NotSupportedException($"The precision {precision} is not supported.");
            }
        }
    }
}
