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

static void Test(int num_objects, int dependencies_num, int const_dependencies_num)
{
	std::default_random_engine generator;
	std::uniform_int_distribution<int> distribution(0, num_objects - 1);

	vector<TestObject> vec_obj;
	vec_obj.resize(num_objects);
	TestObject* obj = &vec_obj[0];
	for (int i = 0; i < num_objects; i++)
	{
		obj[i].id_ = i;
		for (int j = 0; j < dependencies_num; j++)
		{
			obj[i].dependencies_.emplace_back(obj + distribution(generator));
		}
		for (int j = 0; j < const_dependencies_num; j++)
		{
			obj[i].const_dependencies_.emplace_back(obj + distribution(generator));
		}
	}

	unordered_set<IThreadSafeObject*> all_objects;
	all_objects.reserve(num_objects);
	for (int i = 0; i < num_objects; i++)
	{
		all_objects.emplace(obj + i);
	}

	std::chrono::system_clock::time_point time_0 = std::chrono::system_clock::now();
	const unordered_set<shared_ptr<Cluster>> clusters = Cluster::GenerateClasters(all_objects);
	std::chrono::system_clock::time_point time_1 = std::chrono::system_clock::now();
	const vector<GroupOfConcurrentClusters> groups = GroupOfConcurrentClusters::GenerateClasterGroups(clusters);
	std::chrono::system_clock::time_point time_2 = std::chrono::system_clock::now();

	std::cout << "GenerateClasters [ms]: " << std::chrono::duration_cast<std::chrono::milliseconds>(time_1 - time_0).count() << std::endl;
	std::cout << "GenerateClasterGroups [ms]: " << std::chrono::duration_cast<std::chrono::milliseconds>(time_2 - time_1).count() << std::endl;

	std::cout << "clusters: " << clusters.size() << std::endl;
	std::cout << "groups: " << groups.size() << std::endl;
	/*
	for (unsigned int group_idx = 0; group_idx < groups.size(); group_idx++)
	{
		const GroupOfConcurrentClusters& group = groups[group_idx];
		std::cout << group_idx << "[" << group.size() << "]\t ";
		for (unsigned int cluster_idx = 0; cluster_idx < group.size(); cluster_idx++)
		{
			auto& cluster = group[cluster_idx];
			std::cout << cluster->objects_.size()
				//<< "(" << cluster->const_dependencies_clusters_.size() << ")"
				<< " ";
		}
		std::cout << std::endl;
	}
	*/
	std::cout << std::endl;
}

void main()
{
	std::cout << "num_objects: ";
	int num_objects = 2048;
	std::cin >> num_objects; 

	std::cout << "dependencies_num: ";
	int dependencies_num = 3;
	std::cin >> dependencies_num;

	std::cout << "const_dependencies_num: ";
	int const_dependencies_num = 3;
	std::cin >> const_dependencies_num;
	std::cout << std::endl;
	
	for (int i = 0; i < 16; i++)
	{
		Test(num_objects, dependencies_num, const_dependencies_num);
	}
	
	getchar();
	getchar();
}