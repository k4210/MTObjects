#include "IThreadSafeObject.h"
#include <vector>
#include <algorithm>
#include <random>
#include <iostream>
#include <chrono>

using namespace MTObjects;
using std::vector;

class TestObject : public IThreadSafeObject
{
public:
	vector<TestObject*> dependencies_;
	vector<const TestObject*> const_dependencies_;

	int id_ = -1;

	void IsDependentOn(vector<IThreadSafeObject*>& ref_dependencies) const override
	{
		ref_dependencies.insert(ref_dependencies.end(), dependencies_.begin(), dependencies_.end());
	}

	void IsConstDependentOn(unordered_set<const	IThreadSafeObject*>& ref_dependencies) const
	{
		ref_dependencies.insert(const_dependencies_.begin(), const_dependencies_.end());
	}

	void Task()
	{
		//...
	}
};

static void Test(int num_objects, int forced_clusters_num, int dependencies_num, int const_dependencies_num)
{
	vector<TestObject> vec_obj(num_objects);
	{
		vector<vector<TestObject*>> forced_clusters(forced_clusters_num);
		std::default_random_engine generator;
		std::uniform_int_distribution<int> cluster_distribution(0, forced_clusters_num - 1);

		std::uniform_int_distribution<int> const_dependency_distribution(0, num_objects - 1);

		for (int i = 0; i < num_objects; i++)
		{
			TestObject& obj = vec_obj[i];
			obj.id_ = i;
			const int forced_cluster_idx = cluster_distribution(generator);
			vector<TestObject*>& cluster = forced_clusters[forced_cluster_idx];
			const int actual_dependency_num = std::min<int>(dependencies_num, cluster.size());
			if (actual_dependency_num > 0)
			{
				std::uniform_int_distribution<int> dependency_distribution(0, cluster.size() - 1);
				for (int j = 0; j < dependencies_num; j++)
				{
					auto dep_obj = cluster[dependency_distribution(generator)];
					obj.dependencies_.emplace_back(dep_obj);
				}
			}
			cluster.push_back(&obj);

			for (int j = 0; j < const_dependencies_num; j++)
			{
				auto const_dep_obj = &vec_obj[const_dependency_distribution(generator)];
				obj.const_dependencies_.emplace_back(const_dep_obj);
			}
		}
	}

	std::cout << "Test was generated.  " << std::endl;

	unordered_set<IThreadSafeObject*> all_objects;
	all_objects.reserve(num_objects);
	for (int i = 0; i < num_objects; i++)
	{
		all_objects.emplace(&vec_obj[i]);
	}

	std::chrono::system_clock::time_point time_0 = std::chrono::system_clock::now();
	const unordered_set<shared_ptr<Cluster>> clusters = Cluster::GenerateClusters(all_objects);
	std::chrono::system_clock::time_point time_1 = std::chrono::system_clock::now();
	std::cout << "GenerateClusters [ms]: " << std::chrono::duration_cast<std::chrono::milliseconds>(time_1 - time_0).count() << std::endl;

	std::chrono::system_clock::time_point time_2 = std::chrono::system_clock::now();
	const vector<GroupOfConcurrentClusters> groups = GroupOfConcurrentClusters::GenerateClusterGroups(clusters);
	std::chrono::system_clock::time_point time_3 = std::chrono::system_clock::now();
	std::cout << "GenerateClusterGroups [ms]: " << std::chrono::duration_cast<std::chrono::milliseconds>(time_3 - time_2).count() << std::endl;

	std::cout << "clusters: " << clusters.size() << std::endl;
	std::cout << "groups: " << groups.size() << std::endl;
	/*
	for (unsigned int group_idx = 0; group_idx < groups.size(); group_idx++)
	{
		const GroupOfConcurrentClusters& group = groups[group_idx];
		std::cout << group_idx << "[" << group.size() << "]\t ";
		const bool print_inside_group = false;
		if (print_inside_group)
		{
			for (unsigned int cluster_idx = 0; cluster_idx < group.size(); cluster_idx++)
			{
				auto& cluster = group[cluster_idx];
				std::cout << cluster->objects_.size()
					//<< "(" << cluster->const_dependencies_clusters_.size() << ")"
					<< " ";
			}
		}
		std::cout << std::endl;
	}
	*/
	std::cout << std::endl;
}

void main()
{
	int num_objects = 1024 * 1024;
	int forced_clusters = 1024;
	int dependencies_num = 4;
	int const_dependencies_num = 1;
	const bool read_user_input = false;
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
	
	for (int i = 0; i < 16; i++)
	{
		Test(num_objects, forced_clusters, dependencies_num, const_dependencies_num);
	}
	
	getchar();
	getchar();
}