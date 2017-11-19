#include "IThreadSafeObject.h"
#include <vector>
#include <algorithm>
#include <random>
#include <iostream>
#include <chrono>

using namespace MTObjects;
using std::vector;

#ifdef TEST_STUFF 
unsigned int TestStuff::cluster_in_obj_overwritten;
std::size_t TestStuff::max_num_objects_to_handle;
std::size_t TestStuff::max_num_clusters;
unsigned int TestStuff::max_num_data_chunks_used;
#endif //TEST_STUFF

SmartStackStuff::DataChunkMemoryPool64 SmartStackStuff::DataChunkMemoryPool64::instance;

class TestObject : public IThreadSafeObject
{
public:
	vector<TestObject*> dependencies_;
	vector<const TestObject*> const_dependencies_;

	int fake_data[31];

	int id_ = -1;

	void IsDependentOn(FastContainer<IThreadSafeObject*>& ref_dependencies) const override
	{
		ContainerFunc::Insert(ref_dependencies, dependencies_);
		//ref_dependencies.Insert(dependencies_.begin(), dependencies_.end());
	}

	void IsConstDependentOn(unordered_set<const Cluster*>& ref_dependencies) const override
	{
		//ContainerFunc::Insert(ref_dependencies, const_dependencies_);
		//ref_dependencies.insert(const_dependencies_.begin(), const_dependencies_.end());
		std::transform(const_dependencies_.begin(), const_dependencies_.end(), std::inserter(ref_dependencies, ref_dependencies.begin()),
			[](const IThreadSafeObject* o) { return o->cluster_; });
	}

	void Task() override
	{
		IThreadSafeObject::Task();
	}
};

static vector<TestObject> GenerateObjects(int num_objects, int forced_clusters_num, int dependencies_num, int const_dependencies_num, std::default_random_engine& generator)
{
	std::cout << "Generating objects..." << std::endl;

	vector<TestObject> vec_obj;
	vector<vector<TestObject*>> forced_clusters;

	vec_obj.clear();
	vec_obj.resize(num_objects);
	const bool use_forced_clusters = forced_clusters_num > 1;
	if (use_forced_clusters)
	{
		forced_clusters.clear();
		forced_clusters.resize(forced_clusters_num);
	}

	for (int i = 0; i < num_objects; i++)
	{
		TestObject& obj = vec_obj[i];
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
				auto dep_obj = &vec_obj[dependency_distribution(generator)];
				obj.dependencies_.emplace_back(dep_obj);
			}

			for (int j = 0; j < const_dependencies_num; j++)
			{
				auto const_dep_obj = &vec_obj[dependency_distribution(generator)];
				obj.const_dependencies_.emplace_back(const_dep_obj);
			}
		}
	}

	std::cout << "Objects were generated." << std::endl;

	return vec_obj;
}

static vector<IThreadSafeObject*> ShuffleObjects(vector<TestObject>& vec_obj)
{
	vector<IThreadSafeObject*> all_objects;
	all_objects.reserve(vec_obj.size());
	for (unsigned int i = 0; i < vec_obj.size(); i++)
	{
		all_objects.emplace_back(&vec_obj[i]);
	}

	std::mt19937 randomizer;
	std::shuffle(all_objects.begin(), all_objects.end(), randomizer);
	return all_objects;
}

static long long Test(vector<IThreadSafeObject*> all_objects, vector<Cluster>& preallocated_clusters, bool test_group, bool verbose)
{
	std::cout << std::endl;
	vector<Cluster*> clusters;
	long long ms = 0;

	{
		std::chrono::system_clock::time_point time_0 = std::chrono::system_clock::now();

		clusters = Cluster::GenerateClusters(all_objects, preallocated_clusters);

		std::chrono::system_clock::time_point time_1 = std::chrono::system_clock::now();
		std::chrono::system_clock::duration duration = time_1 - time_0;
		ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
		std::cout << "GenerateClusters [ms]: " << ms << std::endl;
		std::cout << "clusters: " << clusters.size();
	}
	if (verbose)
	{
		std::cout << " : ";
		for (auto cluster : clusters)
		{
			std::cout << cluster->objects_.size() << " \t";
		}
	}
	std::cout << std::endl;

#ifdef TEST_STUFF 
	Cluster::Test_AreClustersCoherent(clusters);
#endif //TEST_STUFF

	if (test_group)
	{
		vector<GroupOfConcurrentClusters> groups;
		{
			std::chrono::system_clock::time_point time_1 = std::chrono::system_clock::now();

			groups = GroupOfConcurrentClusters::GenerateClusterGroups(clusters);

			std::chrono::system_clock::time_point time_2 = std::chrono::system_clock::now();
			std::chrono::system_clock::duration duration = time_2 - time_1;
			auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
			ms += duration_ms;
			std::cout << "GenerateClusterGroups [ms]: " << duration_ms << std::endl;
			std::cout << "groups: " << groups.size() << std::endl;
			if (verbose)
			{
				for (unsigned int group_idx = 0; group_idx < groups.size(); group_idx++)
				{
					const GroupOfConcurrentClusters& group = groups[group_idx];
					std::cout << group_idx << "[" << group.clusters_.size() << "]\t ";

					for (unsigned int cluster_idx = 0; cluster_idx < group.clusters_.size(); cluster_idx++)
					{
						auto& cluster = group.clusters_[cluster_idx];
						std::cout << cluster->objects_.size()
							//<< "(" << cluster->const_dependencies_clusters_.size() << ")"
							<< "\t ";
					}
					std::cout << std::endl;
				}
			}
		}

		{
			std::chrono::system_clock::time_point time_1 = std::chrono::system_clock::now();

			for (auto& group : groups)
			{
				concurrency::parallel_for_each(group.clusters_.begin(), group.clusters_.end(), 
					[](Cluster* cluster)
				{
					for (auto obj : cluster->objects_)
					{
						obj->Task();
					}
					cluster->Reset();
				});
			}

			std::chrono::system_clock::time_point time_2 = std::chrono::system_clock::now();
			std::chrono::system_clock::duration duration = time_2 - time_1;
			auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
			ms += duration_ms;
			std::cout << "Execution [ms]: " << duration_ms << std::endl;
		}

	}
	return ms;
}

void main()
{
	const bool read_user_input = false;

	int num_objects = 64 * 1024;
	int forced_clusters = 64;
	int dependencies_num = 16;
	int const_dependencies_num = 8;

#ifdef TEST_STUFF 
	bool verbose = true;
	int repeat_test = 1;
#else
	bool verbose = false;
	int repeat_test = 32;
#endif

	bool test_group = true;
	if (read_user_input)
	{
		std::cout << "num_objects: ";
		std::cin >> num_objects;
		std::cout << "forced_clusters: ";
		std::cin >> forced_clusters;
		std::cout << "dependencies_num: ";
		std::cin >> dependencies_num;
		std::cout << "const_dependencies_num: ";
		std::cin >> const_dependencies_num;
	}
	else
	{
		std::cout << "num_objects: " << num_objects << std::endl;
		std::cout << "forced_clusters: " << forced_clusters << std::endl;
		std::cout << "dependencies_num: " << dependencies_num << std::endl;
		std::cout << "const_dependencies_num: " << const_dependencies_num << std::endl;
	}
	std::cout << std::endl;
	
	std::default_random_engine generator;
	auto objects = GenerateObjects(num_objects, forced_clusters, dependencies_num, const_dependencies_num, generator);
	auto shuffled_objects = ShuffleObjects(objects);

	vector<Cluster> preallocated_clusters(shuffled_objects.size() / 64);

	long long all_time_ns = 0;
	for (int i = 0; i < repeat_test; i++)
	{
		std::cout << "Test: " << i << std::endl;

		all_time_ns += Test(shuffled_objects, preallocated_clusters, test_group, verbose);
#ifdef TEST_STUFF 
		std::cout << "cluster_in_obj_overwritten: " << TestStuff::cluster_in_obj_overwritten << std::endl;
		TestStuff::cluster_in_obj_overwritten = 0;
		std::cout << "max_num_data_chunks_used: " << TestStuff::max_num_data_chunks_used << std::endl;
		std::cout << "max_num_clusters: " << TestStuff::max_num_clusters << std::endl;
#endif //TEST_STUFF
		std::cout << std::endl;
	}
	std::cout << "Average time [ms]: " << all_time_ns / repeat_test << std::endl;
#ifdef TEST_STUFF 
	std::cout << "objects_to_handle: " << TestStuff::max_num_objects_to_handle << std::endl;
	TestStuff::cluster_in_obj_overwritten = 0;
#endif //TEST_STUFF
	getchar();
}