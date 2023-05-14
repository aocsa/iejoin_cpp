//
// Created by Alexander Ocsa on 11/05/23.
//
#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <algorithm>
#include <fmt/format.h>
#include <glog/logging.h>

#include "utils/pprint.h"

// Define the PostComment struct
struct PostComment {
  int id;
  std::string review;
  int postId;
};

std::ostream& operator << (std::ostream& os, const PostComment& pc) {
  os << fmt::format("ID: {}, Review: {}, Post ID: {}\n", pc.id, pc.review, pc.postId);
  return os;
}

// Define the Post struct
struct Post {
  int id;
  std::string title;
};

std::ostream & operator << (std::ostream & os, const Post & p) {
  os << "ID: " << p.id << ", Title: " << p.title << std::endl;
  return os;
}

void MergeJoin(std::vector<Post> posts, std::vector<PostComment> postComments) {

  // Sort posts
  std::sort(posts.begin(), posts.end(), [](const Post &p1, const Post &p2) {
    return p1.id < p2.id;
  });

  // Sort postComments
  std::sort(postComments.begin(), postComments.end(), [](const PostComment &pc1, const PostComment &pc2) {
    if(pc1.postId != pc2.postId) {
      return pc1.postId < pc2.postId;
    } else {
      return pc1.id < pc2.id;
    }
  });

  std::cout << "Sorted postComments\n" << postComments <<  std::endl;
  std::cout << "Sorted\n" << posts <<  std::endl;

  std::vector<std::map<std::string, std::string>> tuples;

  int postCount = posts.size(), postCommentCount = postComments.size();
  int i = 0, j = 0;

  while(i < postCount && j < postCommentCount) {
    Post post = posts[i];
    PostComment postComment = postComments[j];

    if(post.id == postComment.postId) {
      std::map<std::string, std::string> tuple;
      tuple["post_id"] = std::to_string(postComment.postId);
      //      tuple["post_title"] = post.title;
      //      tuple["review"] = postComment.review;

      tuples.push_back(tuple);
      j++;
    } else {
      i++;
    }
  }
  std::cout << tuples << std::endl;
}

TEST(MergeJoinTest, sample) {
  // Initialize the Google Logging library
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;

  google::InitGoogleLogging("iejoin_log");
  // Create a vector of PostComment
  std::vector<PostComment> postComments = {
      {3, "I enjoyed reading this.", 2},
      {4, "Interesting perspective!", 2},
      {1, "Great post!", 1},
      {2, "Very helpful information.", 1},
      {5, "I didn't find this useful.", 3}
  };

  // Create a vector of Post
  std::vector<Post> posts = {
      {1, "from aocsa"},
      {2, "from carlos"},
      {3, "from juan"}
  };
  // Log some messages
  LOG(INFO) << "This is an informational message.";
  LOG(WARNING) << postComments;
  LOG(ERROR) << posts;

  MergeJoin(posts, postComments);
}