#pragma once

#include <unordered_set>
#include <algorithm>
#include <ppl.h>
#include "Utils.h"

namespace MTObjects
{
using std::unordered_set;
template<typename T> using FastContainer = SmartStack<T>;
struct Cluster;

struct IThreadSafeObject
{
public:
	TIndex cluster_ = kNullIndex;

	virtual void IsDependentOn(FastContainer<IThreadSafeObject*>& ref_dependencies) const = 0;
	virtual void IsConstDependentOn(unordered_set<const Cluster*>& ref_dependencies, const vector<Cluster>& clusters) const = 0;

	virtual void Task()
	{
		cluster_ = kNullIndex;
	}
};

struct Cluster
{
private:
	FastContainer<IThreadSafeObject*> objects_;

public:
	void Reset()
	{
		GetObjects().clear();
	}

	FastContainer<IThreadSafeObject*>& GetObjects()
	{
		return objects_;
	}

	const FastContainer<IThreadSafeObject*>& GetObjects() const
	{
		return objects_;
	}

public:
	Cluster() = default;
	~Cluster() = default;
	Cluster(Cluster&& other) = default;
	Cluster& operator=(Cluster&& other) = default;

	Cluster(const Cluster&) = delete;
	Cluster& operator=(const Cluster&) = delete;

public:
	static void CreateClusters(const vector<IThreadSafeObject *> &all_objects, vector<Cluster>& clusters)
	{
		for(unsigned int first_remaining_obj_index = 0; first_remaining_obj_index < all_objects.size(); first_remaining_obj_index++)
		{
			IThreadSafeObject* const initial_object = all_objects[first_remaining_obj_index];
			if(kNullIndex != initial_object->cluster_)
				continue;

			TIndex cluster_index = static_cast<TIndex>(clusters.size());
			Cluster* const initial_cluster = &clusters.emplace_back();
			Cluster* actual_cluster = initial_cluster;
			FastContainer<IThreadSafeObject*> objects_to_handle;
			objects_to_handle.push_back(initial_object);
			while (!objects_to_handle.empty())
			{
				IThreadSafeObject* obj = objects_to_handle.back();
				objects_to_handle.pop_back();
				if (kNullIndex == obj->cluster_)
				{
					actual_cluster->GetObjects().push_back(obj);
					obj->cluster_ = cluster_index;
					obj->IsDependentOn(objects_to_handle);
				}
				else if (obj->cluster_ != cluster_index)
				{
					const TIndex other_cluster = obj->cluster_;
					const bool use_new_cluster = clusters[other_cluster].GetObjects().size() > actual_cluster->GetObjects().size();
					Cluster& to_merge = clusters[use_new_cluster ? cluster_index : other_cluster];
					cluster_index = use_new_cluster ? other_cluster : cluster_index;
					actual_cluster = &clusters[cluster_index];

					for (auto object_merged : to_merge.GetObjects())
					{
						object_merged->cluster_ = cluster_index;
					}
					ContainerFunc::Merge(actual_cluster->GetObjects(), to_merge.GetObjects());

					if (&to_merge == initial_cluster)
					{
						clusters.pop_back();
					}
				}
			}
		}
	}

	static vector<unordered_set<const Cluster*>> CreateClustersDependencies(vector<Cluster>& clusters)
	{
		const auto num_clusters = clusters.size();
		vector<unordered_set<const Cluster*>> const_dependencies_clusters;
		const_dependencies_clusters.resize(num_clusters);
		
		concurrency::parallel_for<size_t>(0, num_clusters, [&clusters, &const_dependencies_clusters](size_t idx)
		{
			auto& cluster = clusters[idx];
			auto& const_dependency_set = const_dependencies_clusters[idx];
			for (auto obj : cluster.GetObjects())
			{
				Assert(obj->cluster_ == idx);
				obj->IsConstDependentOn(const_dependency_set, clusters);
			}
			const_dependency_set.erase(&cluster);
		});

		return const_dependencies_clusters;
	}

#ifdef TEST_STUFF 
	static bool Test_AreClustersCoherent(const vector<Cluster>& clusters)
	{
		// All dependencies of the objects must be inside the cluster
		for (int idx = 0; idx < clusters.size(); idx++)
		{
			auto& cluster = clusters[idx];
			auto& objects = cluster.GetObjects();
			for (auto obj : objects)
			{
				Assert(obj && obj->cluster_ == idx);

				FastContainer<IThreadSafeObject*> dependencies;
				obj->IsDependentOn(dependencies);
				for (auto dep : dependencies)
				{
					Assert(dep && dep->cluster_ == idx);
				}
			}
			vector<IThreadSafeObject*> objects_vec(objects.begin(), objects.end());
			std::sort(objects_vec.begin(), objects_vec.end());
			for (auto iter = objects_vec.begin();;)
			{
				auto obj = *iter;
				iter++;
				if (iter == objects_vec.end())
					break;
				Assert(obj != *iter);
			}
		}
		return true;
	}
#endif //TEST_STUFF
};

static_assert(sizeof(Cluster) == sizeof(FastContainer<IThreadSafeObject*>));

struct GroupOfConcurrentClusters
{
	vector<Cluster*> clusters_;
	unordered_set<const Cluster*> const_dependencies_clusters_;

	void AddCluster(Cluster& cluster, unordered_set<const Cluster*>& cluster_dependencies)
	{
		clusters_.emplace_back(&cluster);
		const_dependencies_clusters_.insert(cluster_dependencies.begin(), cluster_dependencies.end());
	}

	static vector<GroupOfConcurrentClusters> GenerateClusterGroups(vector<Cluster>& clusters)
	{
		auto fits_into_group = [&](const GroupOfConcurrentClusters& group, const Cluster* cluster, const unordered_set<const Cluster*>& cluster_dependencies) -> bool
		{
			return std::none_of(group.clusters_.begin(), group.clusters_.end(), [&](const Cluster* cluster_in_group)
			{
				return cluster_dependencies.end() != cluster_dependencies.find(cluster_in_group);
			}) 
			&& group.const_dependencies_clusters_.find(cluster) == group.const_dependencies_clusters_.end();
		};

		auto dependency_sets = Cluster::CreateClustersDependencies(clusters);

		vector<GroupOfConcurrentClusters> groups;
		groups.reserve(clusters.size()/4);
		groups.resize(1); //groups.resize(std::max<size_t>(1, clusters.size() / 32));
		for (int cluster_index = 0; cluster_index < clusters.size(); ++cluster_index)
		{
			auto cluster = &clusters[cluster_index];
			auto& dependency_set = dependency_sets[cluster_index];
			bool fits_in_existing_group = false;
			{
				const size_t group_num = groups.size();
				const size_t first_group_to_try = cluster_index % group_num;
				for (size_t groups_checked = 0; groups_checked < group_num && !fits_in_existing_group; groups_checked++)
				{
					const size_t group_idx = (first_group_to_try + groups_checked) % group_num;
					auto& group = groups[group_idx];
					if (fits_into_group(group, cluster, dependency_set))
					{
						group.AddCluster(*cluster, dependency_set);
						fits_in_existing_group = true;
					}
				}
			}
			if (!fits_in_existing_group)
			{
				GroupOfConcurrentClusters new_group;
				new_group.AddCluster(*cluster, dependency_set);
				groups.emplace_back(new_group);
			}
		}
		return groups;
	}
};
}
