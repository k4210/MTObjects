#include "IThreadSafeObject.h"
#include <vector>
#include <algorithm>
#include <random>
#include <iostream>
#include <chrono>

using namespace MTObjects;
using std::vector;

SmartStackStuff::DataChunkMemoryPool64 SmartStackStuff::DataChunkMemoryPool64::instance;

class TestObject : public IThreadSafeObject
{
public:
	vector<TestObject*> dependencies_;
	vector<const TestObject*> const_dependencies_;

	int id_ = -1;

	void IsDependentOn(FastContainer<IThreadSafeObject*>& ref_dependencies) const override
	{
		ref_dependencies.Insert(*(vector<IThreadSafeObject*>*)&dependencies_);
	}

	void IsConstDependentOn(IndexSet& ref_dependencies) const override
	{
		for (auto obj : const_dependencies_)
		{
			ref_dependencies[obj->GetClusterIndex()] = true;
		}
	}

	void Task() override
	{
	}
};

static vector<TestObject*> GenerateObjects(int num_objects, int forced_clusters_num, int dependencies_num, int const_dependencies_num, std::default_random_engine& generator)
{
	std::cout << "Generating objects..." << std::endl;

	vector<TestObject*> vec_obj;
	vector<vector<TestObject*>> forced_clusters;

	vec_obj.resize(num_objects);
	for (int i = 0; i < num_objects; i++)
	{
		vec_obj[i] = new TestObject();
	}
	const bool use_forced_clusters = forced_clusters_num > 1;
	if (use_forced_clusters)
	{
		forced_clusters.clear();
		forced_clusters.resize(forced_clusters_num);
	}

	for (int i = 0; i < num_objects; i++)
	{
		TestObject& obj = *vec_obj[i];
		obj.id_ = i;
		if (use_forced_clusters)
		{
			{
				std::uniform_int_distribution<int> cluster_distribution(0, forced_clusters_num - 1);
				const int forced_cluster_idx = cluster_distribution(generator);
				vector<TestObject*>& cluster = forced_clusters[forced_cluster_idx];
				const size_t actual_dependency_num = std::min<size_t>(dependencies_num, cluster.size());
				if (actual_dependency_num > 0)
				{
					std::uniform_int_distribution<size_t> dependency_distribution(0, cluster.size() - 1);
					for (int j = 0; j < dependencies_num; j++)
					{
						auto dep_obj = cluster[dependency_distribution(generator)];
						obj.dependencies_.emplace_back(dep_obj);
					}
				}
				cluster.push_back(&obj);
			}

			{
				//Const deps goes only to the first num_of_const_dep_sources clusters
				const int num_of_const_dep_sources = 8;
				std::uniform_int_distribution<size_t> const_dep_source_dependency_distribution(0, num_of_const_dep_sources);
				const auto forced_cluster_idx = const_dep_source_dependency_distribution(generator);
				const vector<TestObject*>& cluster = forced_clusters[forced_cluster_idx];
				const size_t actual_dependency_num = std::min<size_t>(const_dependencies_num, cluster.size());
				if (actual_dependency_num > 0)
				{
					std::uniform_int_distribution<size_t> dependency_distribution(0, cluster.size() - 1);
					for (int j = 0; j < const_dependencies_num; j++)
					{
						auto dep_obj = cluster[dependency_distribution(generator)];
						obj.const_dependencies_.emplace_back(dep_obj);
					}
				}
			}
		}
		else
		{
			std::uniform_int_distribution<int> dependency_distribution(0, num_objects - 1);
			for (int j = 0; j < dependencies_num; j++)
			{
				auto dep_obj = vec_obj[dependency_distribution(generator)];
				obj.dependencies_.emplace_back(dep_obj);
			}

			for (int j = 0; j < const_dependencies_num; j++)
			{
				auto const_dep_obj = vec_obj[dependency_distribution(generator)];
				obj.const_dependencies_.emplace_back(const_dep_obj);
			}
		}
	}

	std::cout << "Objects were generated." << std::endl;

	return vec_obj;
}

static vector<IThreadSafeObject*> ShuffleObjects(vector<TestObject*>& vec_obj)
{
	vector<IThreadSafeObject*> all_objects;
	all_objects.reserve(vec_obj.size());
	for (unsigned int i = 0; i < vec_obj.size(); i++)
	{
		all_objects.emplace_back(vec_obj[i]);
	}

	std::mt19937 randomizer;
	std::shuffle(all_objects.begin(), all_objects.end(), randomizer);
	return all_objects;
}

static long long Test(const vector<IThreadSafeObject*>& all_objects, ClusterArray& clusters, bool verbose)
{
	std::cout << std::endl;
	long long ms = 0;
	int num_clusters = 0;
	{
		std::chrono::system_clock::time_point time_0 = std::chrono::system_clock::now();

		num_clusters = Cluster::CreateClusters(all_objects, clusters);

		std::chrono::system_clock::time_point time_1 = std::chrono::system_clock::now();
		std::chrono::system_clock::duration duration = time_1 - time_0;
		ms = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
		std::cout << "GenerateClusters [ms]: " << ms << std::endl;

		if (verbose)
		{
			std::cout << " : ";
			int num_empty = 0;
			for (auto& cluster : clusters)
			{
				if (cluster.GetObjects().empty())
				{
					num_empty++;
				}
				else if (verbose)
				{
					std::cout << cluster.GetObjects().size() << " \t";
				}
			}
			std::cout << std::endl << "AllClusters: " << clusters.size()
				<< " Empty: " << num_empty
				<< " Clusters: " << (clusters.size() - num_empty)
				<< std::endl;
		}
	}

	IF_TEST_STUFF(Cluster::Test_AreClustersCoherent(clusters, num_clusters));

	vector<IndexSet> dependency_sets;
	{
		std::chrono::system_clock::time_point time_1 = std::chrono::system_clock::now();

		dependency_sets = Cluster::CreateClustersDependencies(clusters, num_clusters);

		std::chrono::system_clock::time_point time_2 = std::chrono::system_clock::now();
		std::chrono::system_clock::duration duration = time_2 - time_1;
		auto duration_ms = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
		ms += duration_ms;
		std::cout << "CreateClustersDependencies [ms]: " << duration_ms << std::endl;
	}

	vector<GroupOfConcurrentClusters> groups;
	{
		std::chrono::system_clock::time_point time_1 = std::chrono::system_clock::now();

		groups = GroupOfConcurrentClusters::GenerateClusterGroups(clusters, dependency_sets);

		std::chrono::system_clock::time_point time_2 = std::chrono::system_clock::now();
		std::chrono::system_clock::duration duration = time_2 - time_1;
		auto duration_ms = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
		ms += duration_ms;
		std::cout << "GenerateClusterGroups [ms]: " << duration_ms << std::endl;
		if (verbose)
		{
			std::cout << "groups: " << groups.size() << std::endl;
			for (unsigned int group_idx = 0; group_idx < groups.size(); group_idx++)
			{
				const GroupOfConcurrentClusters& group = groups[group_idx];
				std::cout << group_idx << "[" << group.clusters_.size() << "]\t ";

				for (unsigned int cluster_idx = 0; cluster_idx < group.clusters_.size(); cluster_idx++)
				{
					auto& cluster = group.clusters_[cluster_idx];
					std::cout << cluster->GetObjects().size() << "\t ";
				}
				std::cout << std::endl;
			}
		}
	}

	{
		std::chrono::system_clock::time_point time_1 = std::chrono::system_clock::now();

		for (auto& group : groups)
		{
			group.ExecuteGroup();
		}

		std::chrono::system_clock::time_point time_2 = std::chrono::system_clock::now();
		std::chrono::system_clock::duration duration = time_2 - time_1;
		auto duration_ms = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
		ms += duration_ms;
		std::cout << "Execution [ms]: " << duration_ms << std::endl;
	}

	return ms;
}

void main()
{
	constexpr int num_objects = 64 * 1024;
	constexpr int forced_clusters = 64;
	constexpr int dependencies_num = 16;
	constexpr int const_dependencies_num = 8;

#ifdef TEST_STUFF 
	bool verbose = true;
	int repeat_test = 1;
#else
	bool verbose = false;
	int repeat_test = 2048;
#endif

	std::cout << "num_objects: " << num_objects << std::endl;
	std::cout << "forced_clusters: " << forced_clusters << std::endl;
	std::cout << "dependencies_num: " << dependencies_num << std::endl;
	std::cout << "const_dependencies_num: " << const_dependencies_num << std::endl;

	std::cout << std::endl;
	
	std::default_random_engine generator;
	auto objects = GenerateObjects(num_objects, forced_clusters, dependencies_num, const_dependencies_num, generator);
	auto shuffled_objects = ShuffleObjects(objects);

	ClusterArray clusters;
	long long all_time_ns = 0;
	for (auto obj : shuffled_objects)
	{
		obj->Task();
	}
#ifndef TEST_STUFF
	Test(shuffled_objects, clusters, verbose); // to cache the stuff
#endif // TEST_STUFF
	IF_TEST_STUFF(TestStuff::Reset());
	for (int i = 0; i < repeat_test; i++)
	{
		std::cout << std::endl << "Test: " << i << std::endl;
		all_time_ns += Test(shuffled_objects, clusters, verbose);
	}
	std::cout << std::endl << "Average time [ms]: " << all_time_ns / repeat_test << std::endl;
	IF_TEST_STUFF(std::cout << "max_num_data_chunks_used: " << TestStuff::max_num_data_chunks_used() << std::endl);
	IF_TEST_STUFF(std::cout << "objects_to_handle: " << TestStuff::max_num_objects_to_handle() << std::endl);
	IF_TEST_STUFF(std::cout << "max_num_clusters: " << TestStuff::max_num_clusters() << std::endl);
	IF_TEST_STUFF(std::cout << "num_obj_cluster_overwritten: " << TestStuff::num_obj_cluster_overwritten() << std::endl);
	IF_TEST_STUFF(std::cout << "max_objects_to_merge: " << TestStuff::max_objects_to_merge() << std::endl);
	getchar();
}