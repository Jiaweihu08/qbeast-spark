/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.rand

import scala.util.hashing.MurmurHash3

object WriteSynthetic {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val numPartitions = 100
    val hash =
      (s: String) => (MurmurHash3.bytesHash(s.getBytes) & 0x7fffffff).toDouble / Int.MaxValue

    val rdd = spark.sparkContext.parallelize(1 to numPartitions)
    val partitions = rdd.toDF("partitionId").repartition(numPartitions)

    val df = partitions
      .mapPartitions(_ => {
        val countryRegionNames = Seq(
          "Afghanistan",
          "Albania",
          "Algeria",
          "Andorra",
          "Angola",
          "Antigua & Deps",
          "Argentina",
          "Armenia",
          "Australia",
          "Austria",
          "Azerbaijan",
          "Bahamas",
          "Bahrain",
          "Bangladesh",
          "Barbados",
          "Belarus",
          "Belgium",
          "Belize",
          "Benin",
          "Bhutan",
          "Bolivia",
          "Bosnia Herzegovina",
          "Botswana",
          "Brazil",
          "Brunei",
          "Bulgaria",
          "Burkina",
          "Burundi",
          "Cambodia",
          "Cameroon",
          "Canada",
          "Cape Verde",
          "Central African Rep",
          "Chad",
          "Chile",
          "China",
          "Colombia",
          "Comoros",
          "Congo",
          "Congo {Democratic Rep}",
          "Costa Rica",
          "Croatia",
          "Cuba",
          "Cyprus",
          "Czech Republic",
          "Denmark",
          "Djibouti",
          "Dominica",
          "Dominican Republic",
          "East Timor",
          "Ecuador",
          "Egypt",
          "El Salvador",
          "Equatorial Guinea",
          "Eritrea",
          "Estonia",
          "Ethiopia",
          "Fiji",
          "Finland",
          "France",
          "Gabon",
          "Gambia",
          "Georgia",
          "Germany",
          "Ghana",
          "Greece",
          "Grenada",
          "Guatemala",
          "Guinea",
          "Guinea-Bissau",
          "Guyana",
          "Haiti",
          "Honduras",
          "Hungary",
          "Iceland",
          "India",
          "Indonesia",
          "Iran",
          "Iraq",
          "Ireland {Republic}",
          "Israel",
          "Italy",
          "Ivory Coast",
          "Jamaica",
          "Japan",
          "Jordan",
          "Kazakhstan",
          "Kenya",
          "Kiribati",
          "Korea North",
          "Korea South",
          "Kosovo",
          "Kuwait",
          "Kyrgyzstan",
          "Laos",
          "Latvia",
          "Lebanon",
          "Lesotho",
          "Liberia",
          "Libya",
          "Liechtenstein",
          "Lithuania",
          "Luxembourg",
          "Macedonia",
          "Madagascar",
          "Malawi",
          "Malaysia",
          "Maldives",
          "Mali",
          "Malta",
          "Marshall Islands",
          "Mauritania",
          "Mauritius",
          "Mexico",
          "Micronesia",
          "Moldova",
          "Monaco",
          "Mongolia",
          "Montenegro",
          "Morocco",
          "Mozambique",
          "Myanmar, {Burma}",
          "Namibia",
          "Nauru",
          "Nepal",
          "Netherlands",
          "New Zealand",
          "Nicaragua",
          "Niger",
          "Nigeria",
          "Norway",
          "Oman",
          "Pakistan",
          "Palau",
          "Panama",
          "Papua New Guinea",
          "Paraguay",
          "Peru",
          "Philippines",
          "Poland",
          "Portugal",
          "Qatar",
          "Romania",
          "Russian Federation",
          "Rwanda",
          "St Kitts & Nevis",
          "St Lucia",
          "Saint Vincent & the Grenadines",
          "Samoa",
          "San Marino",
          "Sao Tome & Principe",
          "Saudi Arabia",
          "Senegal",
          "Serbia",
          "Seychelles",
          "Sierra Leone",
          "Singapore",
          "Slovakia",
          "Slovenia",
          "Solomon Islands",
          "Somalia",
          "South Africa",
          "South Sudan",
          "Spain",
          "Sri Lanka",
          "Sudan",
          "Suriname",
          "Swaziland",
          "Sweden",
          "Switzerland",
          "Syria",
          "Taiwan",
          "Tajikistan",
          "Tanzania",
          "Thailand",
          "Togo",
          "Tonga",
          "Trinidad & Tobago",
          "Tunisia",
          "Turkey",
          "Turkmenistan",
          "Tuvalu",
          "Uganda",
          "Ukraine",
          "United Arab Emirates",
          "United Kingdom",
          "United States",
          "Uruguay",
          "Uzbekistan",
          "Vanuatu",
          "Vatican City",
          "Venezuela",
          "Vietnam",
          "Yemen",
          "Zambia",
          "Zimbabwe")

        val sortedNames = countryRegionNames.map(s => (s, hash(s))).sortBy(_._2).map(_._1)
        (sortedNames
          .slice(0, sortedNames.size / 16) // 12
          .flatMap(s => (1 to 70000).map(i => C2(s, i))) ++
          sortedNames
            .slice(sortedNames.size / 16, sortedNames.size / 8) // 12
            .flatMap(s => (1 to 3000).map(i => C2(s, i))) ++
          sortedNames
            .slice(sortedNames.size / 8, sortedNames.size / 4) // 25
            .flatMap(s => (1 to 800).map(i => C2(s, i))) ++
          sortedNames
            .slice(sortedNames.size / 4, sortedNames.size / 2) // 49
            .flatMap(s => (1 to 400).map(i => C2(s, i))) ++
          sortedNames
            .slice(sortedNames.size / 2, sortedNames.size) // 98
            .flatMap(s => (1 to 200).map(i => C2(s, i)))).iterator
      })
      .withColumn("random_value", rand())

    val targetPath = "s3a://qbeast-research/synthetic-cr/piecewiseSeq/10k/"

    df.write
      .format("qbeast")
      .option("cubeSize", "10000")
      .option("columnsToIndex", "a,b")
      .save(targetPath)

//    val metrics = QbeastTable.forPath(spark, targetPath).getIndexMetrics()

    // scalastyle:off println
//    println(metrics)
  }

}

case class C2(a: String, b: Int)
