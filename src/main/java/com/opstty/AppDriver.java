package com.opstty;

import com.opstty.job.*;

import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", DistinctArrondissements.class,
                    "A map/reduce program that counts the words in the input files.");

            programDriver.addClass("distinctarrondissements", DistinctArrondissements.class,
                    "A map/reduce program that finds distinct arrondissements containing trees.");

            programDriver.addClass("distinctspecies", DistinctSpecies.class,
                    "A map/reduce program that finds distinct trees species.");
            programDriver.addClass("treekindcount", TreeKindCount.class,
                    "A map/reduce program that calculates the number of trees of each kind (species).");

            programDriver.addClass("maxheightperkind", MaxHeightPerKind.class,
                    "A map/reduce program that calculates the height of the tallest tree of each kind (species).");

            programDriver.addClass("sortheight", SortHeight.class,
                    "A map/reduce program that sorts the trees' heights from smallest to largest.");

            programDriver.addClass("oldesttree", OldestTree.class,
                    "A map/reduce program that displays the district where the oldest tree is.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}

