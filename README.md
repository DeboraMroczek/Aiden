//input data
double[][] points = { {1, 1}, {2, 1}, {1, 2}, {2, 2}, {3, 3}, {8, 8},{9, 8}, {8, 9}, {9, 9} };
 
//vectorize the input data and prepare the input files.
List<Vector> vectors = new ArrayList<Vector>();
for (int i = 0; i < points.length; i++) {
    double[] fr = points[i];
    Vector vec = new NamedVector(new RandomAccessSparseVector(fr.length), "point"+ i);
    vec.assign(fr);
    vectors.add(vec);
}
SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, new Path("clustering/testdata/points/file1"),
        LongWritable.class, VectorWritable.class);
long recNum = 0;
VectorWritable vec = new VectorWritable();
for (Vector point : vectors) {
    vec.set(point);
    writer.append(new LongWritable(recNum++), vec);
}
writer.close();
 
//choose two random clusters to begin with
Path path = new Path("clustering/testdata/clusters/part-00000");
SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Kluster.class);
int k = 2;
for (int i = 0; i < k; i++) {
    Vector vec = vectors.get(7 - i);
    Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
    writer.append(new Text(cluster.getIdentifier()), cluster);
}
writer.close();
 
//run the k-means algorithm
KMeansDriver.run(conf,
        new Path("clustering/testdata/points"),
        new Path("clustering/testdata/clusters"),
        new Path("clustering/output"),
        0.001, 10, true, 0.001, true);
 
SequenceFile.Reader reader = new SequenceFile.Reader(fs,
        new Path("clustering/output/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-0"), conf);
 
IntWritable key = new IntWritable();
WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
while (reader.next(key, value)) {
    System.out.println(value.toString() + " belongs to cluster " + key.toString());
}
reader.close();
